import pytest_asyncio
from httpx import AsyncClient
from httpx._transports.asgi import ASGITransport

from src.api.app import app


@pytest_asyncio.fixture(scope="function")
async def api_client():
    """Создает новый API клиент для каждого теста"""
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://testserver", timeout=30.0
    ) as client:
        yield client
