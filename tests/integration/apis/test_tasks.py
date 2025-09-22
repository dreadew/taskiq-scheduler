import asyncio
import uuid

import pytest

from src.core.enums import TaskStatus
from tests.config import config
from tests.fixtures.integration.api_client import api_client  # noqa: F401


def make_task_payload():
    return {
        "url": config.ASYNC_DSN,
        "ddl": [{"statement": "CREATE TABLE IF NOT EXISTS test (id INT);"}],
        "queries": [
            {
                "queryid": str(uuid.uuid4()),
                "query": "SELECT 1;",
                "runquantity": 1,
                "executiontime": 10,
            }
        ],
        "priority": 3,
    }


@pytest.mark.integration
@pytest.mark.asyncio
async def test_create_task(api_client):
    """
    Проверка эндпоинта создания задачи
    :param api_client: клиент FastAPI
    """

    response = await api_client.post("/tasks/new", json=make_task_payload())
    assert response.status_code == 200
    data = response.json()
    assert "execution_id" in data
    assert "status" in data


@pytest.mark.xfail
@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_task_status(api_client):
    """
    Проверка эндпоинта получения статуса задачи
    :param api_client: клиент FastAPI
    """

    response = await api_client.post("/tasks/new", json=make_task_payload())
    data = response.json()

    await asyncio.sleep(0.1)

    response = await api_client.get(
        "/tasks/status", params={"execution_id": data["execution_id"]}
    )
    assert response.status_code == 200

    status_data = response.json()

    assert "status" in status_data
    assert status_data["status"] in {
        TaskStatus.SCHEDULED.value,
        TaskStatus.RUNNING.value,
        TaskStatus.DONE.value,
    }


@pytest.mark.xfail
@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_task_result(api_client):
    """
    Проверка эндпоинта получения результата задачи
    :param api_client: клиент FastAPI
    """

    response = await api_client.post("/tasks/new", json=make_task_payload())
    data = response.json()

    await asyncio.sleep(0.1)

    response = await api_client.get(
        "/tasks/getresult", params={"execution_id": data["execution_id"]}
    )

    assert response.status_code == 200
    result_data = response.json()

    assert "result" in result_data
    assert isinstance(result_data.get("result"), dict)
