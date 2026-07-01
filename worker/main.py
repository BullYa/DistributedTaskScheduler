import asyncio
import uuid
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Worker Node")

# Identifikator ovog workera (generira se pri pokretanju)
WORKER_ID = str(uuid.uuid4())[:8]

# Evidencija zadataka na ovom workeru {task_id: task_data}
active_tasks: dict = {}


class TaskRequest(BaseModel):
    task_id: str
    name: str
    payload: str


class TaskResult(BaseModel):
    task_id: str
    worker_id: str
    status: str
    result: Optional[str] = None


async def process_task(task_id: str, name: str, payload: str):
    """Asinkrono izvršavanje zadatka — ne blokira event loop."""
    print(f"[WORKER-{WORKER_ID}] Počinjem: {task_id} | {name}")

    active_tasks[task_id]["status"] = "in_progress"

    # Simulacija obrade (asyncio.sleep ne blokira primanje novih zadataka)
    await asyncio.sleep(5)

    active_tasks[task_id]["status"] = "completed"
    active_tasks[task_id]["result"] = f"Zadatak '{name}' izvršen. Payload: {payload}"

    print(f"[WORKER-{WORKER_ID}] Završio: {task_id}")


@app.post("/execute", response_model=TaskResult)
async def execute_task(task: TaskRequest):
    active_tasks[task.task_id] = {
        "task_id": task.task_id,
        "name": task.name,
        "payload": task.payload,
        "status": "received",
        "result": None
    }

    # Pokreni obradu asinkrono — odmah vraćamo odgovor scheduleru
    asyncio.create_task(process_task(task.task_id, task.name, task.payload))

    return TaskResult(
        task_id=task.task_id,
        worker_id=WORKER_ID,
        status="accepted",
        result=f"Worker {WORKER_ID} preuzeo zadatak"
    )


@app.get("/status/{task_id}", response_model=TaskResult)
async def get_task_status(task_id: str):
    if task_id not in active_tasks:
        raise HTTPException(status_code=404, detail="Zadatak nije pronađen na ovom workeru")
    return active_tasks[task_id]


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
