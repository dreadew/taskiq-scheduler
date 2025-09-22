from uuid import uuid4

import pytest

from src.application.schemas.tasks import DDLStatement, QueryItem
from src.application.services.task_service import TaskService
from src.core.enums import TaskStatus
from tests.config import config
from tests.fixtures.integration.task_service import task_service  # noqa: F401


@pytest.mark.xfail
@pytest.mark.asyncio
@pytest.mark.integration
async def test_enqueue_task_integration(task_service: TaskService):
    """
    Интеграционный тест для проверки создания задачи в очереди
    :param task_service: сервис задач
    """

    execution_id = await task_service.enqueue_task(
        dsn=config.SYNC_DSN,
        ddl=[DDLStatement(statement="CREATE TABLE IF NOT EXISTS test (id INT);")],
        queries=[
            QueryItem(
                queryid=uuid4(), query="SELECT 1;", runquantity=1, executiontime=10
            )
        ],
        priority=3,
    )

    execution = await task_service.get_task_execution(execution_id)
    assert execution is not None
    assert execution.status in {TaskStatus.SCHEDULED, TaskStatus.RUNNING}
