from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from src.application.schemas.tasks import (
    TaskResultResponse,
    TaskRunRequest,
    TaskRunResponse,
    TaskStatusResponse,
    TaskStopRequest,
)
from src.application.services.task_service import TaskService
from src.infra.db.sqlalchemy.models.entities import Task, TaskExecution
from src.infra.db.sqlalchemy.session import AsyncSessionLocal
from src.infra.queues.taskiq_task_queue import TaskQueue
from src.infra.repos.base_repo import BaseRepository

router = APIRouter(prefix="/tasks", tags=["Tasks"])


def get_task_service() -> TaskService:
    task_repo = BaseRepository(Task, AsyncSessionLocal)
    task_execution_repo = BaseRepository(TaskExecution, AsyncSessionLocal)
    task_queue = TaskQueue()
    return TaskService(task_repo, task_execution_repo, task_queue)


@router.post("/new", response_model=TaskRunResponse)
async def run_task(
    request: TaskRunRequest, task_service: TaskService = Depends(get_task_service)
):
    execution_id = await task_service.enqueue_task(
        dsn=request.url,
        ddl=request.ddl,
        queries=request.queries,
        priority=request.priority,
    )

    execution = await task_service.get_task_execution(execution_id)

    return TaskRunResponse(execution_id=execution_id, status=execution.status.name)


@router.post("/cancel", response_model=TaskStopRequest)
async def cancel_task(
    execution_id: UUID = Query(..., description="Идентификатор выполнения задачи"),
    task_service: TaskService = Depends(get_task_service),
):
    await task_service.cancel(execution_id)
    return {}


@router.get("/status", response_model=TaskStatusResponse)
async def get_status(
    execution_id: UUID = Query(..., description="Идентификатор выполнения задачи"),
    task_service: TaskService = Depends(get_task_service),
):
    execution = await task_service.get_task_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="задача не найдена.")

    return TaskStatusResponse(status=execution.status.name)


@router.get("/getresult", response_model=TaskResultResponse)
async def get_result(
    execution_id: UUID = Query(..., description="Идентификатор выполнения задачи"),
    task_service: TaskService = Depends(get_task_service),
):
    execution = await task_service.get_task_execution(execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="задача не найдена.")

    return TaskResultResponse(result=execution.result or {})
