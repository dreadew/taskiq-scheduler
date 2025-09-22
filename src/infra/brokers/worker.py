from src.infra.brokers.nats_broker import nats_broker
from src.infra.tasks import db_task  # noqa: F401

broker = nats_broker
