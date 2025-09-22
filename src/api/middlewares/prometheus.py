import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from src.infra.metrics.fastapi_metrics import REQUEST_COUNT, REQUEST_LATENCY


class PrometheusMiddleware(BaseHTTPMiddleware):
    """
    Middleware для сборка метрик в Prometheus
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        start_time = time.time()
        response: Response = await call_next(request)
        process_time = time.time() - start_time

        endpoint = request.url.path
        method = request.method
        status = str(response.status_code)

        REQUEST_COUNT.labels(method=method, endpoint=endpoint, http_status=status).inc()
        REQUEST_LATENCY.labels(method=method, endpoint=endpoint).observe(process_time)

        return response
