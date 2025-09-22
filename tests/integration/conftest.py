from unittest.mock import patch

import pytest

from tests.config import config as test_config

pytest_plugins = [
    "tests.fixtures.integration.clean_db",
    "tests.fixtures.integration.api_client",
    "tests.fixtures.integration.task_service",
]


@pytest.fixture(autouse=True)
def use_test_config():
    """
    Подменяет основную конфигурацию на тестовую
    """
    with patch("src.core.config.config", test_config):
        with patch("src.infra.brokers.nats_broker.config", test_config):
            yield test_config


@pytest.fixture(autouse=True)
async def auto_clean_db(clean_db):
    """
    Автоматически очищает БД перед и после каждого интеграционного теста
    """
    yield clean_db
