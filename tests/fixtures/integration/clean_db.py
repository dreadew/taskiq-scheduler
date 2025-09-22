import asyncio

import pytest
from sqlalchemy import delete

from src.infra.db.sqlalchemy.models.entities import Task, TaskExecution
from src.infra.db.sqlalchemy.session import AsyncSessionLocal


@pytest.fixture(scope="function")
async def clean_db():
    """
    Очищает таблицы до и после каждого теста.
    """
    async with AsyncSessionLocal() as session:
        await session.execute(delete(TaskExecution))
        await session.execute(delete(Task))
        await session.commit()

    await asyncio.sleep(0.01)

    yield

    await asyncio.sleep(0.01)
    async with AsyncSessionLocal() as session:
        await session.execute(delete(TaskExecution))
        await session.execute(delete(Task))
        await session.commit()
