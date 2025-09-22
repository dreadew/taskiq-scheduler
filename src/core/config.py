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
    SYNC_DSN = os.getenv("SYNC_DSN")

    CONN_TYPE = os.getenv("CONN_TYPE", "amqp").lower()

    AMQP_USER = os.getenv("AMQP_USER", "guest")
    AMQP_PASSWORD = os.getenv("AMQP_PASSWORD", "guest")
    AMQP_HOST = os.getenv("AMQP_HOST", "localhost")
    AMQP_PORT = int(os.getenv("AMQP_PORT", 5679))

    CELERY_CONN_STR = (
        f"{CONN_TYPE}://{AMQP_USER}:{AMQP_PASSWORD}@{AMQP_HOST}:{AMQP_PORT}//"
    )
    CELERY_BACKEND_CONN_STR = f"db+{SYNC_DSN}"

    EXTERNAL_DB_CACHE_SIZE = int(os.getenv("EXTERNAL_DB_CACHE_SIZE", 25))
    INTERNAL_DB_POOL_SIZE = int(os.getenv("INTERNAL_DB_POOL_SIZE", 10))
    INTERNAL_DB_MAX_OVERFLOW = int(os.getenv("INTERNAL_DB_MAX_OVERFLOW", 15))

    CELERY_TASK_DEFAULT_PRIORITY = int(os.getenv("CELERY_TASK_DEFAULT_PRIORITY", 3))
    CELERY_TASK_MAX_RETRIES = int(os.getenv("CELERY_TASK_MAX_RETRIES", 3))
    CELERY_TASK_TIME_LIMIT = int(os.getenv("CELERY_TASK_TIME_LIMIT", 1200))

    METRICS_PORT = int(os.getenv("METRICS_PORT", 5040))
    CELERY_METRICS_PORT = int(os.getenv("CELERY_METRICS_PORT", 5060))


config = Config()
