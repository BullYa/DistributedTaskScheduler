from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from collections import deque
import uuid
import asyncio
import httpx
import time

# red zadataka - drži ID-eve zadataka koji čekaju na workera
task_queue: deque = deque()

# sve info o zadacima
tasks: dict = {}

# registrirani workeri {worker_id: {url, last_heartbeat, active_tasks, status}}
workers: dict = {}

HEARTBEAT_TIMEOUT = 15  # ako worker ne javi u 15s - mrtav


async def check_workers():
    """Svake 5 sekundi provjeri jesu li workeri još živi."""
    while True:
        await asyncio.sleep(5)
        now = time.time()
        for worker_id, info in list(workers.items()):
            if now - info["last_heartbeat"] > HEARTBEAT_TIMEOUT:
                if info["status"] != "dead":
                    print(f"[SCHEDULER] Worker {worker_id} ne javlja se, označen kao mrtav")
                    workers[worker_id]["status"] = "dead"


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(check_workers())
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


def pick_worker():
    """Vraća prvog dostupnog (živog) workera. Load balancing dolazi u sljedećoj fazi."""
    for worker_id, info in workers.items():
        if info["status"] == "alive":
            return worker_id, info
    return None, None


@app.post("/register")
async def register_worker(data: WorkerRegister):
    workers[data.worker_id] = {
        "url": data.url,
        "last_heartbeat": time.time(),
        "active_tasks": 0,
        "status": "alive"
    }
    print(f"[SCHEDULER] Worker registriran: {data.worker_id} @ {data.url}")
    return {"message": "ok"}


@app.post("/heartbeat")
async def heartbeat(data: HeartbeatRequest):
    if data.worker_id not in workers:
        raise HTTPException(status_code=404, detail="Worker nije registriran")
    workers[data.worker_id]["last_heartbeat"] = time.time()
    workers[data.worker_id]["active_tasks"] = data.active_tasks
    workers[data.worker_id]["status"] = "alive"
    return {"message": "ok"}


@app.post("/submit-task", response_model=TaskResponse)
async def submit_task(task: TaskRequest):
    task_id = str(uuid.uuid4())

    task_data = {
        "task_id": task_id,
        "name": task.name,
        "payload": task.payload,
        "status": "queued",
        "worker_id": None
    }
    task_queue.append(task_id)
    tasks[task_id] = task_data

    print(f"[SCHEDULER] novi zadatak: {task_id} | {task.name}")

    # pokušaj odmah poslati workeru
    worker_id, worker_info = pick_worker()
    if worker_id:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    f"{worker_info['url']}/execute",
                    json={
                        "task_id": task_id,
                        "name": task.name,
                        "payload": task.payload
                    },
                    timeout=5.0
                )
            if resp.status_code == 200:
                try:
                    task_queue.remove(task_id)
                except ValueError:
                    pass
                tasks[task_id]["status"] = "dispatched"
                tasks[task_id]["worker_id"] = worker_id
                print(f"[SCHEDULER] Zadatak {task_id} poslan workeru {worker_id}")
        except Exception as e:
            print(f"[SCHEDULER] Greška pri slanju workeru: {e}")
            # zadatak ostaje u redu

    status = tasks[task_id]["status"]
    if worker_id and status == "dispatched":
        msg = f"Zadatak '{task.name}' poslan workeru {worker_id}"
    else:
        msg = f"Zadatak '{task.name}' čeka u redu (nema dostupnih workera)"

    return TaskResponse(task_id=task_id, status=status, message=msg)


@app.get("/tasks")
async def get_tasks():
    return {
        "u_redu": len(task_queue),
        "svi": list(tasks.values())
    }


@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Zadatak nije pronađen")
    return tasks[task_id]


@app.get("/workers")
async def get_workers():
    return {"workeri": workers}


@app.get("/health")
async def health():
    alive = sum(1 for w in workers.values() if w["status"] == "alive")
    return {"status": "ok", "service": "scheduler", "aktivni_workeri": alive}
