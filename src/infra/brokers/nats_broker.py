from taskiq import SimpleRetryMiddleware
from taskiq_nats import NatsBroker
from taskiq_postgresql import PostgresqlResultBackend

from src.core.config import config
from src.core.logging import get_logger

logger = get_logger(__name__)


def create_nats_broker() -> NatsBroker:
    """
    Создает и настраивает NATS брокер для Taskiq.

    :return: настроенный NATS брокер
    """
    logger.info(f"Создание NATS брокера: {config.NATS_URL}")

    result_backend = PostgresqlResultBackend(
        dsn=config.CLEAN_DSN,
    )

    broker = NatsBroker(
        servers=config.NATS_URL, subject=config.NATS_QUEUE_NAME
    ).with_result_backend(result_backend)

    broker.add_middlewares(
        SimpleRetryMiddleware(default_retry_count=config.TASKIQ_MAX_RETRIES),
    )

    logger.info("NATS брокер успешно создан")
    return broker


nats_broker = create_nats_broker()
