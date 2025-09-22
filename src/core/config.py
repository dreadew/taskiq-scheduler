import os

from dotenv import load_dotenv

load_dotenv()
if os.path.exists(".env.local"):
    load_dotenv(".env.local", override=True)


class Config:
    """
    Конфигурация приложения
    """

    APP_NAME = os.getenv("APP_NAME")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    ASYNC_DSN = os.getenv("ASYNC_DSN")
    CLEAN_DSN = os.getenv("CLEAN_DSN")

    NATS_HOST = os.getenv("NATS_HOST", "localhost")
    NATS_PORT = int(os.getenv("NATS_PORT", 4222))
    NATS_URL = os.getenv("NATS_URL", f"nats://{NATS_HOST}:{NATS_PORT}")
    NATS_QUEUE_NAME = os.getenv("NATS_QUEUE_NAME", "task_queue")

    EXTERNAL_DB_CACHE_SIZE = int(os.getenv("EXTERNAL_DB_CACHE_SIZE", 25))
    INTERNAL_DB_POOL_SIZE = int(os.getenv("INTERNAL_DB_POOL_SIZE", 10))
    INTERNAL_DB_MAX_OVERFLOW = int(os.getenv("INTERNAL_DB_MAX_OVERFLOW", 15))

    TASKIQ_DEFAULT_PRIORITY = int(os.getenv("TASKIQ_DEFAULT_PRIORITY", 3))
    TASKIQ_MAX_RETRIES = int(os.getenv("TASKIQ_MAX_RETRIES", 3))
    TASKIQ_TIME_LIMIT = int(os.getenv("TASKIQ_TIME_LIMIT", 1200))

    METRICS_PORT = int(os.getenv("METRICS_PORT", 5040))
    TASKIQ_METRICS_PORT = int(os.getenv("TASKIQ_METRICS_PORT", 5060))

    CIRCUIT_BREAKER_FAILURE_THRESHOLD = int(
        os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", 5)
    )
    CIRCUIT_BREAKER_RECOVERY_TIMEOUT = int(
        os.getenv("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", 60)
    )
    CIRCUIT_BREAKER_EXPECTED_EXCEPTION_TIMEOUT = int(
        os.getenv("CIRCUIT_BREAKER_EXPECTED_EXCEPTION_TIMEOUT", 30)
    )


config = Config()
