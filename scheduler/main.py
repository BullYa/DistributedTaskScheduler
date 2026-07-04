from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from collections import deque
import uuid
import asyncio
import httpx
import time

# red zadataka - ID-evi zadataka koji čekaju na workera
task_queue: deque = deque()

# sve info o zadacima
tasks: dict = {}

# registrirani workeri {worker_id: {url, last_heartbeat, active_tasks, status}}
workers: dict = {}

HEARTBEAT_TIMEOUT = 15   # ako worker ne javi u 15s smatramo ga mrtvim
DISPATCH_INTERVAL = 2    # svako toliko sekundi pokušaj isprazniti red


def pick_worker():
    """Least-loaded: uzmi živog workera s najmanje aktivnih zadataka."""
    alive = {wid: info for wid, info in workers.items() if info["status"] == "alive"}
    if not alive:
        return None, None
    worker_id = min(alive, key=lambda wid: alive[wid]["active_tasks"])
    return worker_id, alive[worker_id]


async def dispatch_task(task_id: str) -> bool:
    """Pošalji jedan zadatak najboljem workeru. Vraća True ako je uspjelo."""
    if task_id not in tasks:
        return False

    worker_id, worker_info = pick_worker()
    if not worker_id:
        return False

    task = tasks[task_id]
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{worker_info['url']}/execute",
                json={
                    "task_id": task_id,
                    "name": task["name"],
                    "payload": task["payload"]
                },
                timeout=5.0
            )
        if resp.status_code == 200:
            tasks[task_id]["status"] = "dispatched"
            tasks[task_id]["worker_id"] = worker_id
            # odmah povećaj brojač da least-loaded radi točno i prije novog heartbeata
            workers[worker_id]["active_tasks"] += 1
            print(f"[SCHEDULER] {task_id} -> worker {worker_id}")
            return True
    except Exception as e:
        print(f"[SCHEDULER] slanje workeru {worker_id} nije uspjelo: {e}")
        # pao je između dva heartbeata - proglasi ga mrtvim i spasi njegove zadatke
        workers[worker_id]["status"] = "dead"
        requeue_tasks_from(worker_id)
    return False


async def dispatch_loop():
    """Pozadinska petlja - prazni red čekanja kad ima dostupnih workera."""
    while True:
        await asyncio.sleep(DISPATCH_INTERVAL)
        # šalji dok ima zadataka i dok slanje uspijeva
        while task_queue:
            task_id = task_queue[0]
            ok = await dispatch_task(task_id)
            if ok:
                task_queue.popleft()
            else:
                break  # nema workera ili slanje ne ide, probaj opet za DISPATCH_INTERVAL


async def check_workers():
    """Provjerava heartbeat i spašava zadatke s mrtvih workera."""
    while True:
        await asyncio.sleep(5)
        now = time.time()
        for worker_id, info in list(workers.items()):
            if info["status"] == "alive" and now - info["last_heartbeat"] > HEARTBEAT_TIMEOUT:
                print(f"[SCHEDULER] worker {worker_id} ne javlja se - proglašen mrtvim")
                workers[worker_id]["status"] = "dead"
                requeue_tasks_from(worker_id)


def requeue_tasks_from(worker_id: str):
    """Sve nedovršene zadatke mrtvog workera vrati u red čekanja."""
    rescued = 0
    for task_id, task in tasks.items():
        if task.get("worker_id") == worker_id and task["status"] == "dispatched":
            task["status"] = "queued"
            task["worker_id"] = None
            task_queue.append(task_id)
            rescued += 1
    if rescued:
        print(f"[SCHEDULER] spašeno {rescued} zadataka s workera {worker_id}, vraćeni u red")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(check_workers())
    asyncio.create_task(dispatch_loop())
    yield


app = FastAPI(title="Task Scheduler", lifespan=lifespan)


class TaskRequest(BaseModel):
    name: str
    payload: str


class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


class WorkerRegister(BaseModel):
    worker_id: str
    url: str


class HeartbeatRequest(BaseModel):
    worker_id: str
    active_tasks: int


class TaskCompleted(BaseModel):
    task_id: str
    worker_id: str
    result: str


@app.post("/register")
async def register_worker(data: WorkerRegister):
    workers[data.worker_id] = {
        "url": data.url,
        "last_heartbeat": time.time(),
        "active_tasks": 0,
        "status": "alive"
    }
    print(f"[SCHEDULER] worker registriran: {data.worker_id} @ {data.url}")
    return {"message": "ok"}


@app.post("/heartbeat")
async def heartbeat(data: HeartbeatRequest):
    if data.worker_id not in workers:
        raise HTTPException(status_code=404, detail="worker nije registriran")
    w = workers[data.worker_id]
    w["last_heartbeat"] = time.time()
    w["active_tasks"] = data.active_tasks
    # ako je bio proglašen mrtvim a javio se - oživi ga
    if w["status"] == "dead":
        print(f"[SCHEDULER] worker {data.worker_id} se vratio")
    w["status"] = "alive"
    return {"message": "ok"}


@app.post("/task-completed")
async def task_completed(data: TaskCompleted):
    if data.task_id not in tasks:
        raise HTTPException(status_code=404, detail="nepoznat zadatak")
    task = tasks[data.task_id]
    # ignoriraj rezultat ako je zadatak u međuvremenu re-dodijeljen drugom workeru
    if task.get("worker_id") != data.worker_id:
        print(f"[SCHEDULER] rezultat za {data.task_id} od starog workera {data.worker_id} - ignoriram")
        return {"message": "ignored"}
    task["status"] = "completed"
    task["result"] = data.result
    print(f"[SCHEDULER] zadatak {data.task_id} završen (worker {data.worker_id})")
    return {"message": "ok"}


@app.post("/submit-task", response_model=TaskResponse)
async def submit_task(task: TaskRequest):
    task_id = str(uuid.uuid4())

    tasks[task_id] = {
        "task_id": task_id,
        "name": task.name,
        "payload": task.payload,
        "status": "queued",
        "worker_id": None,
        "result": None
    }

    print(f"[SCHEDULER] novi zadatak: {task_id} | {task.name}")

    # pokušaj odmah, ako ne ide dispatch_loop će ga pokupiti kasnije
    ok = await dispatch_task(task_id)
    if not ok:
        task_queue.append(task_id)
        return TaskResponse(
            task_id=task_id,
            status="queued",
            message=f"Zadatak '{task.name}' čeka u redu (pozicija {len(task_queue)})"
        )

    return TaskResponse(
        task_id=task_id,
        status="dispatched",
        message=f"Zadatak '{task.name}' poslan workeru {tasks[task_id]['worker_id']}"
    )


@app.get("/tasks")
async def get_tasks():
    return {
        "u_redu": len(task_queue),
        "svi": list(tasks.values())
    }


@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="zadatak nije pronađen")
    return tasks[task_id]


@app.get("/workers")
async def get_workers():
    return {"workeri": workers}


@app.get("/health")
async def health():
    alive = sum(1 for w in workers.values() if w["status"] == "alive")
    return {"status": "ok", "service": "scheduler", "aktivni_workeri": alive}
