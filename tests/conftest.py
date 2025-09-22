import asyncio

import pytest


@pytest.fixture(scope="function")
def event_loop():
    """Создает инстанс дефолтного цикла событий для каждого теста"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


pytest_plugins = [
    "tests.fixtures.integration.clean_db",
    "tests.fixtures.integration.api_client",
    "tests.fixtures.integration.task_service",
]
