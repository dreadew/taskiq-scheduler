from prometheus_client import Counter, Histogram

REQUEST_COUNT = Counter(
    "fastapi_request_count",
    "количество обращений к эндпоинтам FastAPI",
    ["method", "endpoint", "http_status"],
)

REQUEST_LATENCY = Histogram(
    "fastapi_request_latency_seconds",
    "время обработки запроса FastAPI",
    ["method", "endpoint"],
)
