import asyncio

import pytest


@pytest.fixture(scope="function")
def event_loop():
    """Создает инстанс дефолтного цикла событий для каждого теста"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


pytest_plugins = []
