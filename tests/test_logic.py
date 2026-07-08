"""Testovi core logike schedulera - pokretanje: pytest iz roota projekta."""
import pytest

from scheduler import logic
from scheduler.logic import TaskStatus, WorkerStatus


@pytest.fixture(autouse=True)
def clean_state():
    """Prije svakog testa ocisti stanje da testovi ne ovise jedan o drugom."""
    logic.task_queue.clear()
    logic.tasks.clear()
    logic.workers.clear()
    yield


def make_worker(worker_id, active=0, status=WorkerStatus.ALIVE):
    logic.workers[worker_id] = {
        "url": f"http://{worker_id}:8001",
        "last_heartbeat": 0,
        "active_tasks": active,
        "status": status,
    }


def make_task(task_id, status=TaskStatus.DISPATCHED, worker_id=None):
    logic.tasks[task_id] = {
        "task_id": task_id,
        "name": "t",
        "payload": "p",
        "status": status,
        "worker_id": worker_id,
        "result": None,
    }


def test_least_loaded_bira_najmanje_opterecenog():
    make_worker("w1", active=3)
    make_worker("w2", active=1)
    make_worker("w3", active=2)
    wid, _ = logic.pick_worker()
    assert wid == "w2"


def test_mrtvi_worker_se_preskace():
    make_worker("w1", active=3)
    make_worker("w2", active=0, status=WorkerStatus.DEAD)  # najmanje posla ali mrtav
    wid, _ = logic.pick_worker()
    assert wid == "w1"


def test_nema_zivih_vraca_none():
    make_worker("w1", status=WorkerStatus.DEAD)
    wid, info = logic.pick_worker()
    assert wid is None and info is None


def test_requeue_spasava_samo_dispatched_od_mrtvog():
    make_task("t1", status=TaskStatus.DISPATCHED, worker_id="w1")
    make_task("t2", status=TaskStatus.COMPLETED, worker_id="w1")   # gotov, ne dirati
    make_task("t3", status=TaskStatus.DISPATCHED, worker_id="w2")  # tudji, ne dirati

    rescued = logic.requeue_tasks_from("w1")

    assert rescued == 1
    assert logic.tasks["t1"]["status"] == TaskStatus.QUEUED
    assert logic.tasks["t1"]["worker_id"] is None
    assert logic.tasks["t2"]["status"] == TaskStatus.COMPLETED
    assert logic.tasks["t3"]["status"] == TaskStatus.DISPATCHED
    assert list(logic.task_queue) == ["t1"]


def test_requeue_bez_zadataka_vraca_nulu():
    make_task("t1", status=TaskStatus.COMPLETED, worker_id="w1")
    assert logic.requeue_tasks_from("w1") == 0
    assert len(logic.task_queue) == 0


@pytest.mark.asyncio
async def test_dispatch_bez_workera_vraca_false():
    make_task("t1", status=TaskStatus.QUEUED)
    ok = await logic.dispatch_task("t1")
    assert ok is False
    assert logic.tasks["t1"]["status"] == TaskStatus.QUEUED


@pytest.mark.asyncio
async def test_dispatch_nepostojeceg_zadatka_vraca_false():
    make_worker("w1")
    ok = await logic.dispatch_task("ne-postoji")
    assert ok is False
