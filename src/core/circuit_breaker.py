import asyncio
import time
from contextlib import asynccontextmanager
from enum import Enum
from typing import Dict

from src.core.config import config
from src.core.logging import get_logger

logger = get_logger(__name__)


class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(Exception):
    """Исключение, когда circuit breaker открыт"""

    pass


class SimpleCircuitBreaker:
    """Простой Circuit Breaker"""

    def __init__(
        self,
        failure_threshold: int = config.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
        recovery_timeout: int = config.CIRCUIT_BREAKER_RECOVERY_TIMEOUT,
        expected_exceptions: tuple = (ConnectionError, TimeoutError),
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exceptions = expected_exceptions

        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitBreakerState.CLOSED

        self._lock = asyncio.Lock()

    async def _should_attempt_reset(self) -> bool:
        """Проверить, нужно ли попытаться сбросить circuit breaker"""
        if self.state != CircuitBreakerState.OPEN:
            return False

        if self.last_failure_time is None:
            return False

        return time.time() - self.last_failure_time >= self.recovery_timeout

    async def _on_success(self):
        """Обработать успешное выполнение"""
        async with self._lock:
            self.failure_count = 0
            self.state = CircuitBreakerState.CLOSED
            logger.info("Circuit breaker сброшен в состояние CLOSED")

    async def _on_failure(self, exception: Exception):
        """Обработать ошибку"""
        async with self._lock:
            if isinstance(exception, self.expected_exceptions):
                self.failure_count += 1
                self.last_failure_time = time.time()

                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logger.warning(
                        f"Circuit breaker открыт! Ошибок: {self.failure_count}/{self.failure_threshold}"
                    )

    @asynccontextmanager
    async def protect(self):
        """Контекст для защиты операций circuit breaker'ом"""

        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if await self._should_attempt_reset():
                    self.state = CircuitBreakerState.HALF_OPEN
                    logger.info("Circuit breaker переключен в состояние HALF_OPEN")
                else:
                    raise CircuitBreakerOpenError(
                        f"Circuit breaker открыт. Попробуйте через {self.recovery_timeout} секунд"
                    )

        try:
            yield
            await self._on_success()
        except Exception as e:
            await self._on_failure(e)
            raise


_circuit_breakers: Dict[str, SimpleCircuitBreaker] = {}


def get_circuit_breaker(dsn: str) -> SimpleCircuitBreaker:
    """Получить circuit breaker для конкретного DSN"""
    if dsn not in _circuit_breakers:
        _circuit_breakers[dsn] = SimpleCircuitBreaker()
        logger.info(f"Создан circuit breaker для DSN: {dsn[:20]}...")
    return _circuit_breakers[dsn]


@asynccontextmanager
async def circuit_breaker_protection(dsn: str):
    """Контекст для защиты операций с внешней БД"""
    cb = get_circuit_breaker(dsn)
    async with cb.protect():
        yield
