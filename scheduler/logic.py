import asyncio
import logging
import os
import time
from collections import deque

import httpx

logger = logging.getLogger("scheduler")

# konfiguracija preko env varijabli, defaulti su ok za lokalno
HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "15"))   # koliko dugo toleriramo tisinu prije nego workera otpisemo
DISPATCH_INTERVAL = int(os.getenv("DISPATCH_INTERVAL", "2"))    # koliko cesto petlja pokusava isprazniti red
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "5"))          # koliko cesto provjeravamo heartbeatove
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "5"))


# statusi kao konstante da se ne tipka string po kodu
class TaskStatus:
    QUEUED = "queued"
    DISPATCHED = "dispatched"
    COMPLETED = "completed"


class WorkerStatus:
    ALIVE = "alive"
    DEAD = "dead"


# stanje sustava, sve u memoriji
task_queue: deque = deque()   # id-evi zadataka koji cekaju
tasks: dict = {}              # evidencija svih zadataka
workers: dict = {}            # workeri koje znamo, zivi i mrtvi


async def post_json(url: str, data: dict, timeout: float = HTTP_TIMEOUT):
    """Mali helper da se ne ponavlja isti httpx blok svugdje."""
    async with httpx.AsyncClient() as client:
        return await client.post(url, json=data, timeout=timeout)


def pick_worker():
    """Bira zivog workera s najmanje posla (least-loaded)."""
    alive = {wid: info for wid, info in workers.items() if info["status"] == WorkerStatus.ALIVE}
    if not alive:
        return None, None
    worker_id = min(alive, key=lambda wid: alive[wid]["active_tasks"])
    return worker_id, alive[worker_id]


def requeue_tasks_from(worker_id: str):
    """Vrati u red sve sto je mrtvi worker ostavio nedovrseno."""
    rescued = 0
    for task_id, task in tasks.items():
        if task.get("worker_id") == worker_id and task["status"] == TaskStatus.DISPATCHED:
            task["status"] = TaskStatus.QUEUED
            task["worker_id"] = None
            task_queue.append(task_id)
            rescued += 1
    if rescued:
        logger.warning("spaseno %d zadataka s workera %s, vraceni u red", rescued, worker_id)
    return rescued


async def dispatch_task(task_id: str) -> bool:
    """Salje zadatak odabranom workeru, True ako je prosao."""
    if task_id not in tasks:
        return False

    worker_id, worker_info = pick_worker()
    if not worker_id:
        return False

    task = tasks[task_id]
    try:
        resp = await post_json(
            f"{worker_info['url']}/execute",
            {"task_id": task_id, "name": task["name"], "payload": task["payload"]},
        )
        if resp.status_code == 200:
            # dok smo cekali odgovor worker je mogao biti proglasen mrtvim,
            # requeue za njega je vec prosao pa ovo ne smijemo oznaciti kao dispatched
            if workers.get(worker_id, {}).get("status") != WorkerStatus.ALIVE:
                logger.warning("worker %s umro tijekom slanja, zadatak ostaje u redu", worker_id)
                return False
            task["status"] = TaskStatus.DISPATCHED
            task["worker_id"] = worker_id
            # rucno uvecamo brojac da ne cekamo heartbeat, inace bi svi zadaci
            # u tih par sekundi zavrsili na istom workeru
            workers[worker_id]["active_tasks"] += 1
            logger.info("%s -> worker %s", task_id, worker_id)
            return True
    except Exception as e:
        logger.warning("slanje workeru %s nije uspjelo: %s", worker_id, e)
        # ocito je pao, spasimo mu zadatke da ne vise u zraku
        workers[worker_id]["status"] = WorkerStatus.DEAD
        requeue_tasks_from(worker_id)
    return False


async def dispatch_loop():
    """Vrti se u pozadini i gura zadatke iz reda prema workerima."""
    while True:
        await asyncio.sleep(DISPATCH_INTERVAL)
        while task_queue:
            task_id = task_queue[0]
            ok = await dispatch_task(task_id)
            if ok:
                task_queue.popleft()
            else:
                break  # trenutno nema kome, probamo opet za par sekundi


async def check_workers():
    """Pazi na heartbeatove, mrtvima uzima zadatke natrag."""
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        now = time.time()
        for worker_id, info in list(workers.items()):
            if info["status"] == WorkerStatus.ALIVE and now - info["last_heartbeat"] > HEARTBEAT_TIMEOUT:
                logger.warning("worker %s ne javlja se - proglasen mrtvim", worker_id)
                workers[worker_id]["status"] = WorkerStatus.DEAD
                requeue_tasks_from(worker_id)
