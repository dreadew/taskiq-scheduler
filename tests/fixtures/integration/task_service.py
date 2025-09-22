import pytest

from src.application.services.task_service import TaskService
from src.infra.db.sqlalchemy.models.entities import Task, TaskExecution
from src.infra.db.sqlalchemy.session import AsyncSessionLocal
from src.infra.queues.celery_task_queue import TaskQueue
from src.infra.repos.base_repo import BaseRepository


@pytest.fixture
def task_service():
    """
    Фикстура с сервисом задач
    :return: сервис задач
    """

    task_repo = BaseRepository(Task, AsyncSessionLocal)
    task_execution_repo = BaseRepository(TaskExecution, AsyncSessionLocal)
    task_queue = TaskQueue()

    return TaskService(task_repo, task_execution_repo, task_queue)
