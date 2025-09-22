import random
import time
from typing import Callable

from src.core.logging import get_logger

logger = get_logger(__name__)


class RetryableError(Exception):
    """Базовый класс для ошибок, которые можно повторить."""

    pass


class ConnectionError(RetryableError):
    """Ошибка подключения к внешней системе."""

    pass


class TimeoutError(RetryableError):
    """Ошибка таймаута."""

    pass


class RetryConfig:
    """Конфигурация для retry логики."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        retryable_exceptions: tuple = (ConnectionError, TimeoutError),
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions


def calculate_delay(attempt: int, config: RetryConfig) -> float:
    """
    Вычисляет задержку для повторной попытки с экспоненциальным backoff.

    :param attempt: номер попытки (начиная с 1)
    :param config: конфигурация retry
    :return: время задержки в секундах
    """
    delay = config.base_delay * (config.exponential_base ** (attempt - 1))
    delay = min(delay, config.max_delay)

    if config.jitter:
        delay = delay * (0.5 + random.random() * 0.5)

    return delay


def retry_with_backoff(config: RetryConfig):
    """
    Декоратор для выполнения функции с retry логикой.

    :param config: конфигурация retry
    """

    def decorator(func: Callable):
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(1, config.max_attempts + 1):
                try:
                    logger.info(
                        f"Попытка {attempt}/{config.max_attempts} выполнения {func.__name__}"
                    )
                    return func(*args, **kwargs)
                except config.retryable_exceptions as e:
                    last_exception = e

                    if attempt == config.max_attempts:
                        logger.error(
                            f"Все {config.max_attempts} попыток исчерпаны для {func.__name__}. "
                            f"Последняя ошибка: {str(e)}"
                        )
                        raise e

                    delay = calculate_delay(attempt, config)
                    logger.warning(
                        f"Попытка {attempt} неудачна для {func.__name__}: {str(e)}. "
                        f"Повтор через {delay:.2f} секунд"
                    )
                    time.sleep(delay)
                except Exception as e:
                    logger.error(f"Неповторяемая ошибка в {func.__name__}: {str(e)}")
                    raise e

            if last_exception:
                raise last_exception

        return wrapper

    return decorator


class DatabaseRetryConfig(RetryConfig):
    """Специализированная конфигурация для БД операций."""

    def __init__(self):
        super().__init__(
            max_attempts=3,
            base_delay=2.0,
            max_delay=30.0,
            exponential_base=2.0,
            jitter=True,
            retryable_exceptions=(
                ConnectionError,
                TimeoutError,
            ),
        )


class TrinoRetryConfig(RetryConfig):
    """Специализированная конфигурация для Trino операций."""

    def __init__(self):
        super().__init__(
            max_attempts=5,
            base_delay=3.0,
            max_delay=120.0,
            exponential_base=1.5,
            jitter=True,
            retryable_exceptions=(
                ConnectionError,
                TimeoutError,
            ),
        )
