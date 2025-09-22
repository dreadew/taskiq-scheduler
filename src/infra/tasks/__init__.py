from prometheus_client import start_http_server

from src.core.config import config


def setup_taskiq_metrics():

    start_http_server(config.TASKIQ_METRICS_PORT)
