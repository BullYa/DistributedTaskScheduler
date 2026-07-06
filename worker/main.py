import asyncio
import uuid
import os
import socket
from typing import Optional
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx

WORKER_ID = str(uuid.uuid4())[:8]
SCHEDULER_URL = os.getenv("SCHEDULER_URL", "http://localhost:8000")
WORKER_PORT = int(os.getenv("WORKER_PORT", "8001"))
# u dockeru je ovo ime containera, lokalno localhost
WORKER_HOST = os.getenv("WORKER_HOST", "localhost")
if WORKER_HOST == "auto":
    # svaki container javi svoj hostname, bitno kad se workeri skaliraju
    WORKER_HOST = socket.gethostname()
WORKER_URL = f"http://{WORKER_HOST}:{WORKER_PORT}"

active_tasks: dict = {}


async def send_heartbeat():
    """Svakih 5 sekundi javimo scheduleru da smo zivi i koliko posla imamo."""
    await asyncio.sleep(3)  # da se scheduler stigne dici
    while True:
        in_progress = sum(1 for t in active_tasks.values() if t["status"] == "in_progress")
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{SCHEDULER_URL}/heartbeat",
                    json={"worker_id": WORKER_ID, "active_tasks": in_progress},
                    timeout=3.0
                )
        except Exception as e:
            print(f"[WORKER-{WORKER_ID}] heartbeat nije uspio: {e}")
        await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # prijavimo se scheduleru, par pokusaja za slucaj da se on jos dize
    for attempt in range(5):
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{SCHEDULER_URL}/register",
                    json={"worker_id": WORKER_ID, "url": WORKER_URL},
                    timeout=3.0
                )
            print(f"[WORKER-{WORKER_ID}] registriran @ {SCHEDULER_URL}")
            break
        except Exception as e:
            print(f"[WORKER-{WORKER_ID}] registracija neuspješna ({attempt + 1}/5): {e}")
            await asyncio.sleep(2)

    asyncio.create_task(send_heartbeat())
    yield


app = FastAPI(title="Worker Node", lifespan=lifespan)


class TaskRequest(BaseModel):
    task_id: str
    name: str
    payload: str


class TaskResult(BaseModel):
    task_id: str
    worker_id: str
    status: str
    result: Optional[str] = None


async def report_completed(task_id: str, result: str):
    """Javimo scheduleru da je zadatak gotov."""
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{SCHEDULER_URL}/task-completed",
                json={"task_id": task_id, "worker_id": WORKER_ID, "result": result},
                timeout=3.0
            )
    except Exception as e:
        print(f"[WORKER-{WORKER_ID}] nisam uspio javiti rezultat za {task_id}: {e}")


async def process_task(task_id: str, name: str, payload: str):
    print(f"[WORKER-{WORKER_ID}] počinjem: {task_id} | {name}")
    active_tasks[task_id]["status"] = "in_progress"

    # simulacija posla, sleep je async pa u meduvremenu normalno primamo nove zadatke
    await asyncio.sleep(5)

    result = f"Zadatak '{name}' izvršen. Payload: {payload}"
    active_tasks[task_id]["status"] = "completed"
    active_tasks[task_id]["result"] = result
    print(f"[WORKER-{WORKER_ID}] završio: {task_id}")

    await report_completed(task_id, result)


@app.post("/execute", response_model=TaskResult)
async def execute_task(task: TaskRequest):
    active_tasks[task.task_id] = {
        "task_id": task.task_id,
        "name": task.name,
        "payload": task.payload,
        "status": "received",
        "result": None
    }

    # obrada ide u pozadini, scheduleru odmah vracamo da smo preuzeli
    asyncio.create_task(process_task(task.task_id, task.name, task.payload))

    return TaskResult(
        task_id=task.task_id,
        worker_id=WORKER_ID,
        status="accepted",
        result=f"worker {WORKER_ID} preuzeo zadatak"
    )


@app.get("/status/{task_id}", response_model=TaskResult)
async def get_task_status(task_id: str):
    if task_id not in active_tasks:
        raise HTTPException(status_code=404, detail="zadatak nije pronađen na ovom workeru")
    t = active_tasks[task_id]
    return TaskResult(
        task_id=t["task_id"],
        worker_id=WORKER_ID,
        status=t["status"],
        result=t.get("result")
    )


@app.get("/status")
async def get_worker_status():
    in_progress = sum(1 for t in active_tasks.values() if t["status"] == "in_progress")
    return {
        "worker_id": WORKER_ID,
        "aktivni_zadaci": in_progress,
        "ukupno_obradeno": len(active_tasks)
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "worker", "worker_id": WORKER_ID}
