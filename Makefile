.PHONY: start-celery-worker, start-celery-flower, start-fastapi, upgrade, downgrade, history

start-celery-worker:
    celery -A src.infra.scheduler.celery_app worker --loglevel=info --concurrency=4

start-celery-flower:
    celery -A src.infra.scheduler.celery_app flower --port=5000 --basic_auth=user:password

start-fastapi:
    uvicorn src.api.main:app --host 0.0.0.0 --port 8080 --reload

upgrade:
    alembic upgrade head

downgrade:
    alembic downgrade 1

history:
    alembic history --verbose