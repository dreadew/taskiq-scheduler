from fastapi import FastAPI, Request
from prometheus_client import start_http_server
from starlette.responses import JSONResponse

from src.api.middlewares.prometheus import PrometheusMiddleware
from src.api.routes import health, tasks
from src.core.config import config

app = FastAPI(title=config.APP_NAME)

app.add_middleware(PrometheusMiddleware)

app.include_router(tasks.router)
app.include_router(health.router)


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )


start_http_server(config.METRICS_PORT)
