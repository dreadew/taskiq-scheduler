import time

from celery.signals import (
    task_failure,
    task_postrun,
    task_prerun,
    task_received,
    task_retry,
)
from prometheus_client import Counter, Histogram

TASK_SUCCESS = Counter(
    "celery_task_success_total",
    "количество успешно выполненных задач в celery",
    ["task_name"],
)
TASK_FAILURE = Counter(
    "celery_task_failure_total", "количество упавших задач в celery", ["task_name"]
)
TASK_RETRY = Counter(
    "celery_task_retry_total",
    "количество повторных запусков задач в celery",
    ["task_name"],
)
TASK_RECEIVED = Counter(
    "celery_task_received_total", "общее количество задач в celery", ["task_name"]
)
TASK_LATENCY = Histogram(
    "celery_task_latency_seconds", "время выполнения задач в celery", ["task_name"]
)

_task_start_times = {}


@task_failure.connect
def task_failure_handler(sender=None, **kwargs):
    """Метрика задач завершенных с ошибкой"""
    TASK_FAILURE.labels(task_name=sender.name).inc()


@task_retry.connect
def task_retry_handler(sender=None, **kwargs):
    """Метрика повторных запусков задач"""
    TASK_RETRY.labels(task_name=sender.name).inc()


@task_received.connect
def task_received_handler(sender=None, **kwargs):
    """Метрика общего количество полученных задач"""
    TASK_RECEIVED.labels(task_name=sender.name).inc()


@task_prerun.connect
def task_started_handler(sender=None, task_id=None, **kwargs):
    """Метрика для сбора времени старта задач"""
    _task_start_times[task_id] = time.time()


@task_postrun.connect
def task_finished_handler(sender=None, task_id=None, **kwargs):
    """Метрика для сбора времени завершения задач"""
    start_time = _task_start_times.pop(task_id, None)
    if start_time is not None:
        duration = time.time() - start_time
        TASK_LATENCY.labels(task_name=sender.name).observe(duration)
        TASK_SUCCESS.labels(task_name=sender.name).inc()
