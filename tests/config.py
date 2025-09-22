import os

from dotenv import load_dotenv

load_dotenv(".env.test")


class Config:
    """
    Конфигурация приложения
    """

    APP_NAME = os.getenv("APP_NAME")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

    ASYNC_DSN = os.getenv("ASYNC_DSN")
    SYNC_DSN = os.getenv("SYNC_DSN")
    CLEAN_DSN = os.getenv("CLEAN_DSN")

    NATS_HOST = os.getenv("NATS_HOST", "nats")
    NATS_PORT = int(os.getenv("NATS_PORT", 4222))
    NATS_URL = os.getenv("NATS_URL", f"nats://{NATS_HOST}:{NATS_PORT}")
    NATS_QUEUE_NAME = os.getenv("NATS_QUEUE_NAME", "test_task_queue")
    NATS_DURABLE_NAME = os.getenv("NATS_DURABLE_NAME", "test_task_scheduler")

    EXTERNAL_DB_CACHE_SIZE = int(os.getenv("EXTERNAL_DB_CACHE_SIZE", 25))
    INTERNAL_DB_POOL_SIZE = int(os.getenv("INTERNAL_DB_POOL_SIZE", 10))
    INTERNAL_DB_MAX_OVERFLOW = int(os.getenv("INTERNAL_DB_MAX_OVERFLOW", 15))

    TASKIQ_DEFAULT_PRIORITY = int(os.getenv("TASKIQ_DEFAULT_PRIORITY", 3))
    TASKIQ_MAX_RETRIES = int(os.getenv("TASKIQ_MAX_RETRIES", 3))
    TASKIQ_TIME_LIMIT = int(os.getenv("TASKIQ_TIME_LIMIT", 1200))

    METRICS_PORT = int(os.getenv("METRICS_PORT", 8000))
    TASKIQ_METRICS_PORT = int(os.getenv("TASKIQ_METRICS_PORT", 8001))


config = Config()
