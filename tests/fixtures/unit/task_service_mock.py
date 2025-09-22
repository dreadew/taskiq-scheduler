from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.application.services.task_service import TaskService


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class FakeAsyncSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    def begin(self):
        return FakeTransaction()

    async def commit(self):
        pass

    async def rollback(self):
        pass

    async def execute(self, *args, **kwargs):
        return None

    async def flush(self):
        pass

    def add(self, obj):
        pass


@pytest.fixture
def task_service_mock():
    """
    Mock TaskService и его зависимости
    """

    task_repo = MagicMock()
    task_repo.session_factory.return_value = FakeAsyncSession()
    task_repo.create = AsyncMock()

    task_execution_repo = MagicMock()
    task_execution_repo.session_factory.return_value = FakeAsyncSession()
    task_execution_repo.create = AsyncMock()
    task_execution_repo.update = AsyncMock()
    task_execution_repo.get = AsyncMock()
    task_execution_repo.get_for_update = AsyncMock()
    task_execution_repo.find_latest_by_field = AsyncMock()

    @asynccontextmanager
    async def fake_transaction():
        yield FakeTransaction()

    task_execution_repo.transaction = fake_transaction

    task_queue = MagicMock()
    task_queue.queue_task = AsyncMock()
    task_queue.cancel_task = AsyncMock()

    service = TaskService(task_repo, task_execution_repo, task_queue)

    return {
        "service": service,
        "task_repo": task_repo,
        "task_execution_repo": task_execution_repo,
        "task_queue": task_queue,
    }
