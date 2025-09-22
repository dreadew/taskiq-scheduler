from celery.signals import worker_process_init
from prometheus_client import start_http_server

from src.core.config import config

# @worker_process_init.connect
# def setup_metrics(**kwargs):
#     start_http_server(config.CELERY_METRICS_PORT)
