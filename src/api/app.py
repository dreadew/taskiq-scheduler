from fastapi import FastAPI, Request
from prometheus_client import start_http_server
from starlette.responses import JSONResponse

from src.api.middlewares.prometheus import PrometheusMiddleware
from src.api.routes import health, tasks
from src.core.config import config
from src.infra.brokers.nats_broker import nats_broker as broker
from src.infra.tasks import db_task  # noqa: F401

app = FastAPI(title=config.APP_NAME)

app.add_middleware(PrometheusMiddleware)

app.include_router(tasks.router)
app.include_router(health.router)


@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске приложения"""
    await broker.startup()


@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при остановке приложения"""
    await broker.shutdown()


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )


start_http_server(config.METRICS_PORT)
