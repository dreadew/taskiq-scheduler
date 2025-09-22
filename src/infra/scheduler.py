from celery import Celery

from src.core.config import config
from src.infra.tasks.db_task import execute_db_task  # noqa: F401

celery_app = Celery(
    "scheduler", broker=config.CELERY_CONN_STR, backend=config.CELERY_BACKEND_CONN_STR
)

celery_app.conf.update(
    task_track_started=True, worker_prefetch_multiplier=1, task_acks_late=True
)
