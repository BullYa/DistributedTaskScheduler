from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from collections import deque
import uuid

app = FastAPI(title="Task Scheduler")

# red zadataka - drži samo ID-eve
task_queue: deque = deque()

# sve info o zadacima
tasks: dict = {}


class TaskRequest(BaseModel):
    name: str
    payload: str


class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str


@app.post("/submit-task", response_model=TaskResponse)
async def submit_task(task: TaskRequest):
    task_id = str(uuid.uuid4())

    task_data = {
        "task_id": task_id,
        "name": task.name,
        "payload": task.payload,
        "status": "queued",
    }

    task_queue.append(task_id)
    tasks[task_id] = task_data

    print(f"[SCHEDULER] novi zadatak: {task_id} | {task.name}")

    return TaskResponse(
        task_id=task_id,
        status="queued",
        message=f"Zadatak '{task.name}' dodan u red. Pozicija: {len(task_queue)}"
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
        raise HTTPException(status_code=404, detail="Zadatak nije pronađen")
    return tasks[task_id]


@app.get("/health")
async def health():
    return {"status": "ok", "service": "scheduler"}
