import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel

from scheduler import logic
from scheduler.logic import TaskStatus, WorkerStatus

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("scheduler")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(logic.check_workers())
    asyncio.create_task(logic.dispatch_loop())
    yield


app = FastAPI(title="Task Scheduler", lifespan=lifespan, docs_url=None)


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


@app.get("/", include_in_schema=False)
async def dashboard():
    # nadzorna ploca, obicna html stranica koja vuce /tasks i /workers
    return FileResponse(os.path.join(os.path.dirname(__file__), "dashboard.html"))


@app.get("/docs", include_in_schema=False)
async def dark_docs():
    # swagger u tamnoj temi da se slaze s plocom
    return FileResponse(os.path.join(os.path.dirname(__file__), "docs_dark.html"))


@app.post("/register", tags=["interno (worker ↔ scheduler)"])
async def register_worker(data: WorkerRegister):
    logic.workers[data.worker_id] = {
        "url": data.url,
        "last_heartbeat": time.time(),
        "active_tasks": 0,
        "status": WorkerStatus.ALIVE,
    }
    logger.info("worker registriran: %s @ %s", data.worker_id, data.url)
    return {"message": "ok"}


@app.post("/heartbeat", tags=["interno (worker ↔ scheduler)"])
async def heartbeat(data: HeartbeatRequest):
    if data.worker_id not in logic.workers:
        raise HTTPException(status_code=404, detail="worker nije registriran")
    w = logic.workers[data.worker_id]
    w["last_heartbeat"] = time.time()
    w["active_tasks"] = data.active_tasks
    # javio se iako smo ga otpisali - vrati ga u igru
    if w["status"] == WorkerStatus.DEAD:
        logger.info("worker %s se vratio", data.worker_id)
    w["status"] = WorkerStatus.ALIVE
    return {"message": "ok"}


@app.post("/task-completed", tags=["interno (worker ↔ scheduler)"])
async def task_completed(data: TaskCompleted):
    if data.task_id not in logic.tasks:
        raise HTTPException(status_code=404, detail="nepoznat zadatak")
    task = logic.tasks[data.task_id]
    # zadatak je u meduvremenu presao drugom workeru, ovaj rezultat vise ne vrijedi
    if task.get("worker_id") != data.worker_id:
        logger.info("rezultat za %s od starog workera %s - ignoriram", data.task_id, data.worker_id)
        return {"message": "ignored"}
    task["status"] = TaskStatus.COMPLETED
    task["result"] = data.result
    # worker sad ima jedan zadatak manje, heartbeat bi to ionako sredio ali cemu cekati
    if data.worker_id in logic.workers and logic.workers[data.worker_id]["active_tasks"] > 0:
        logic.workers[data.worker_id]["active_tasks"] -= 1
    logger.info("zadatak %s zavrsen (worker %s)", data.task_id, data.worker_id)
    return {"message": "ok"}


@app.post("/submit-task", response_model=TaskResponse, tags=["klijent"])
async def submit_task(task: TaskRequest):
    task_id = str(uuid.uuid4())

    logic.tasks[task_id] = {
        "task_id": task_id,
        "name": task.name,
        "payload": task.payload,
        "status": TaskStatus.QUEUED,
        "worker_id": None,
        "result": None,
    }

    logger.info("novi zadatak: %s | %s", task_id, task.name)

    # probamo odmah poslati, ako ne uspije petlja ce ga pokupiti
    ok = await logic.dispatch_task(task_id)
    if not ok:
        logic.task_queue.append(task_id)
        return TaskResponse(
            task_id=task_id,
            status=TaskStatus.QUEUED,
            message=f"Zadatak '{task.name}' čeka u redu (pozicija {len(logic.task_queue)})",
        )

    return TaskResponse(
        task_id=task_id,
        status=TaskStatus.DISPATCHED,
        message=f"Zadatak '{task.name}' poslan workeru {logic.tasks[task_id]['worker_id']}",
    )


@app.get("/tasks", tags=["klijent"])
async def get_tasks():
    return {
        "u_redu": len(logic.task_queue),
        "svi": list(logic.tasks.values()),
    }


@app.get("/tasks/{task_id}", tags=["klijent"])
async def get_task(task_id: str):
    if task_id not in logic.tasks:
        raise HTTPException(status_code=404, detail="zadatak nije pronađen")
    return logic.tasks[task_id]


@app.get("/workers", tags=["klijent"])
async def get_workers():
    return {"workeri": logic.workers}


@app.get("/health", tags=["klijent"])
async def health():
    alive = sum(1 for w in logic.workers.values() if w["status"] == WorkerStatus.ALIVE)
    return {"status": "ok", "service": "scheduler", "aktivni_workeri": alive}


def custom_openapi():
    # mice automatski generirane 422 odgovore iz docsa, samo prave buku
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(title=app.title, version="1.0.0", routes=app.routes)
    for path in schema.get("paths", {}).values():
        for op in path.values():
            op.get("responses", {}).pop("422", None)
    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi
