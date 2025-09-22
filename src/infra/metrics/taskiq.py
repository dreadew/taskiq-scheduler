import time
from typing import Dict

from prometheus_client import Counter, Histogram

TASK_SUCCESS = Counter(
    "taskiq_task_success_total",
    "количество успешно выполненных задач в taskiq",
    ["task_name"],
)
TASK_FAILURE = Counter(
    "taskiq_task_failure_total", "количество упавших задач в taskiq", ["task_name"]
)
TASK_RETRY = Counter(
    "taskiq_task_retry_total",
    "количество повторных запусков задач в taskiq",
    ["task_name"],
)
TASK_RECEIVED = Counter(
    "taskiq_task_received_total", "общее количество задач в taskiq", ["task_name"]
)
TASK_LATENCY = Histogram(
    "taskiq_task_latency_seconds", "время выполнения задач в taskiq", ["task_name"]
)

_task_start_times: Dict[str, float] = {}


def task_started_metrics(task_name: str, task_id: str):
    """Метрика для сбора времени старта задач"""
    _task_start_times[task_id] = time.time()
    TASK_RECEIVED.labels(task_name=task_name).inc()


def task_finished_metrics(task_name: str, task_id: str, success: bool = True):
    """Метрика для сбора времени завершения задач"""
    start_time = _task_start_times.pop(task_id, None)
    if start_time is not None:
        duration = time.time() - start_time
        TASK_LATENCY.labels(task_name=task_name).observe(duration)

    if success:
        TASK_SUCCESS.labels(task_name=task_name).inc()
    else:
        TASK_FAILURE.labels(task_name=task_name).inc()


def task_retry_metrics(task_name: str):
    """Метрика повторных запусков задач"""
    TASK_RETRY.labels(task_name=task_name).inc()
