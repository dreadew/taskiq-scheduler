from uuid import uuid4

import pytest

from src.application.schemas.tasks import DDLStatement, QueryItem
from src.core.enums import TaskStatus
from src.infra.db.sqlalchemy.models.entities import TaskExecution
from tests.fixtures.unit.task_service_mock import task_service_mock  # noqa: F401


@pytest.mark.asyncio
@pytest.mark.unit
async def test_enqueue_task(task_service_mock):
    """
    Тест постановки задачи в очередь
    """

    service = task_service_mock["service"]
    task_repo = task_service_mock["task_repo"]
    task_execution_repo = task_service_mock["task_execution_repo"]
    task_queue = task_service_mock["task_queue"]

    task_repo.create.return_value = uuid4()
    task_execution_repo.create.return_value = uuid4()
    task_execution_repo.find_latest_by_field.return_value = None
    task_queue.queue_task.return_value.id = "taskiq-task-id"

    execution_id = await service.enqueue_task(
        dsn="sqlalchemy://user:pass@localhost/db",
        ddl=[DDLStatement(statement="CREATE TABLE IF NOT EXISTS test (id INT);")],
        queries=[
            QueryItem(
                queryid=uuid4(), query="SELECT 1;", runquantity=1, executiontime=10
            )
        ],
        priority=3,
    )

    task_repo.create.assert_awaited_once()
    task_execution_repo.find_latest_by_field.assert_awaited_once()
    task_execution_repo.create.assert_awaited_once()
    task_execution_repo.update.assert_awaited_once()
    task_queue.queue_task.assert_called_once()
    assert execution_id is not None


@pytest.mark.asyncio
@pytest.mark.unit
async def test_cancel_task(task_service_mock):
    """
    Тест отмены выполнения задачи
    """

    service = task_service_mock["service"]
    task_execution_repo = task_service_mock["task_execution_repo"]
    task_queue = task_service_mock["task_queue"]

    execution_id = uuid4()
    mock_execution = TaskExecution(
        task_id=uuid4(), broker_task_id="broker-task-id", status=TaskStatus.SCHEDULED
    )

    task_execution_repo.get_for_update.return_value = mock_execution
    task_execution_repo.update.return_value = None

    await service.cancel(execution_id)

    task_queue.cancel_task.assert_called_once_with("broker-task-id")

    assert task_execution_repo.update.await_count == 2


@pytest.mark.asyncio
@pytest.mark.unit
async def test_cancel_task_not_found(task_service_mock: dict):
    """
    Тест отмены отсутствующей задачи
    """

    service = task_service_mock["service"]
    task_execution_repo = task_service_mock["task_execution_repo"]

    task_execution_repo.get_for_update.return_value = None

    with pytest.raises(ValueError, match="задача не найдена."):
        await service.cancel(uuid4())


@pytest.mark.asyncio
@pytest.mark.unit
async def test_cancel_task_not_scheduled(task_service_mock: dict):
    """
    Тест остановки завершившейся задчаи
    """

    service = task_service_mock["service"]
    task_execution_repo = task_service_mock["task_execution_repo"]

    task_execution_repo.get.return_value = TaskExecution(
        task_id=uuid4(), broker_task_id="taskiq-task-id", status=TaskStatus.DONE
    )

    with pytest.raises(
        ValueError,
        match="остановить можно задачу только в статусах SCHEDULED или RUNNING.",
    ):
        await service.cancel(uuid4())


@pytest.mark.asyncio
@pytest.mark.unit
async def test_enqueue_task_invalid_dsn(task_service_mock):
    """
    Тест постановки задачи с невалидным DSN
    """

    service = task_service_mock["service"]

    with pytest.raises(ValueError, match="невозможно определить структуру БД."):
        await service.enqueue_task(
            dsn="unsupported//user:pass@localhost/db", ddl=[], queries=[], priority=5
        )


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.unit
async def test_enqueue_task_wrong_priority_positive(task_service_mock):
    """
    Тест постановки задачи в очередь с некорректным приоритетом (positive)
    """

    with pytest.raises(
        ValueError, match="приоритет не может быть больше 9 или меньше 0."
    ):
        service = task_service_mock["service"]
        task_repo = task_service_mock["task_repo"]
        task_execution_repo = task_service_mock["task_execution_repo"]
        task_queue = task_service_mock["task_queue"]

        task_repo.create.return_value = uuid4()
        task_execution_repo.create.return_value = uuid4()
        task_queue.queue_task.return_value.id = "taskiq-task-id"

        execution_id = await service.enqueue_task(
            dsn="sqlalchemy://user:pass@localhost/db",
            ddl=[DDLStatement(statement="CREATE TABLE IF NOT EXISTS test (id INT);")],
            queries=[
                QueryItem(
                    queryid=uuid4(), query="SELECT 1;", runquantity=1, executiontime=10
                )
            ],
            priority=10,
        )

        task_repo.create.assert_awaited_once()
        task_execution_repo.create.assert_awaited_once()
        task_execution_repo.update.assert_awaited_once()
        task_queue.queue_task.assert_called_once()
        assert execution_id is not None


@pytest.mark.asyncio
@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.unit
async def test_enqueue_task_wrong_priority_negative(task_service_mock):
    """
    Тест постановки задачи в очередь с некорректным приоритетом (negative)
    """

    with pytest.raises(
        ValueError, match="приоритет не может быть больше 9 или меньше 0."
    ):
        service = task_service_mock["service"]
        task_repo = task_service_mock["task_repo"]
        task_execution_repo = task_service_mock["task_execution_repo"]
        task_queue = task_service_mock["task_queue"]

        task_repo.create.return_value = uuid4()
        task_execution_repo.create.return_value = uuid4()
        task_queue.queue_task.return_value.id = "taskiq-task-id"

        execution_id = await service.enqueue_task(
            dsn="sqlalchemy://user:pass@localhost/db",
            ddl=[DDLStatement(statement="CREATE TABLE IF NOT EXISTS test (id INT);")],
            queries=[
                QueryItem(
                    queryid=uuid4(), query="SELECT 1;", runquantity=1, executiontime=10
                )
            ],
            priority=-1,
        )

        task_repo.create.assert_awaited_once()
        task_execution_repo.create.assert_awaited_once()
        task_execution_repo.update.assert_awaited_once()
        task_queue.queue_task.assert_called_once()
        assert execution_id is not None


@pytest.mark.asyncio
@pytest.mark.unit
async def test_enqueue_task_with_previous_execution(task_service_mock):
    """
    Тест постановки задачи в очередь с предыдущим выполнением
    """

    service = task_service_mock["service"]
    task_repo = task_service_mock["task_repo"]
    task_execution_repo = task_service_mock["task_execution_repo"]
    task_queue = task_service_mock["task_queue"]

    task_id = uuid4()
    prev_execution_id = uuid4()

    prev_execution = TaskExecution(
        id=prev_execution_id, task_id=task_id, status=TaskStatus.DONE
    )

    task_repo.create.return_value = task_id
    task_execution_repo.create.return_value = uuid4()
    task_execution_repo.find_latest_by_field.return_value = prev_execution
    task_queue.queue_task.return_value.id = "taskiq-task-id"

    execution_id = await service.enqueue_task(
        dsn="sqlalchemy://user:pass@localhost/db",
        ddl=[DDLStatement(statement="CREATE TABLE IF NOT EXISTS test (id INT);")],
        queries=[
            QueryItem(
                queryid=uuid4(), query="SELECT 1;", runquantity=1, executiontime=10
            )
        ],
        priority=3,
    )

    task_execution_repo.find_latest_by_field.assert_awaited_once_with(
        "task_id", task_id
    )

    task_repo.create.assert_awaited_once()
    task_execution_repo.create.assert_awaited_once()
    task_execution_repo.update.assert_awaited_once()
    task_queue.queue_task.assert_called_once()
    assert execution_id is not None


@pytest.mark.asyncio
@pytest.mark.unit
async def test_enqueue_task_without_previous_execution(task_service_mock):
    """
    Тест постановки задачи в очередь без предыдущего выполнения
    """

    service = task_service_mock["service"]
    task_repo = task_service_mock["task_repo"]
    task_execution_repo = task_service_mock["task_execution_repo"]
    task_queue = task_service_mock["task_queue"]

    task_id = uuid4()

    task_repo.create.return_value = task_id
    task_execution_repo.create.return_value = uuid4()
    task_execution_repo.find_latest_by_field.return_value = None
    task_queue.queue_task.return_value.id = "taskiq-task-id"

    execution_id = await service.enqueue_task(
        dsn="sqlalchemy://user:pass@localhost/db",
        ddl=[DDLStatement(statement="CREATE TABLE IF NOT EXISTS test (id INT);")],
        queries=[
            QueryItem(
                queryid=uuid4(), query="SELECT 1;", runquantity=1, executiontime=10
            )
        ],
        priority=3,
    )

    task_execution_repo.find_latest_by_field.assert_awaited_once_with(
        "task_id", task_id
    )

    task_repo.create.assert_awaited_once()
    task_execution_repo.create.assert_awaited_once()
    task_execution_repo.update.assert_awaited_once()
    task_queue.queue_task.assert_called_once()
    assert execution_id is not None
