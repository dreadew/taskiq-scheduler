from functools import lru_cache

import trino
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from src.core.config import config
from src.core.utils.parse_url import get_db_type, parse_trino_jdbc

engine = create_async_engine(
    config.ASYNC_DSN,
    future=True,
    echo=False,
    pool_size=config.INTERNAL_DB_POOL_SIZE,
    max_overflow=config.INTERNAL_DB_MAX_OVERFLOW,
)

AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

sync_engine = create_engine(
    config.SYNC_DSN,
    future=True,
    echo=False,
    pool_size=config.INTERNAL_DB_POOL_SIZE,
    max_overflow=config.INTERNAL_DB_MAX_OVERFLOW,
)

SessionLocal = sessionmaker(sync_engine, class_=Session, expire_on_commit=False)


@lru_cache(maxsize=config.EXTERNAL_DB_CACHE_SIZE)
def get_external_db_session(dsn: str) -> sessionmaker:
    """
    Получить асинхронную сессию с внешней БД
    :param dsn: строка подключения
    :return: асинхронная сессия с внешней БД
    """
    _engine = _get_external_engine_internal(dsn, True)

    _async_session = sessionmaker(_engine, class_=AsyncSession, expire_on_commit=False)

    return _async_session


@lru_cache(maxsize=config.EXTERNAL_DB_CACHE_SIZE)
def get_external_db_session_sync(dsn: str) -> sessionmaker:
    """
    Получить синхронную сессию с внешней БД
    :param dsn: строка подключения
    :return: синхронная сессия с внешней БД
    """
    _engine = _get_external_engine_internal(dsn, False)

    _session = sessionmaker(_engine, class_=Session, expire_on_commit=False)

    return _session


def _get_external_engine_internal(dsn: str, is_async: bool = False):
    """
    Получить engine для подключения к внешней БД
    :param dsn: строка подклюения
    :param is_async: признак асинхронности
    :return: Engine | AsyncEngine
    """

    db_type = get_db_type(dsn)
    if db_type == "trino":
        parsed_url = parse_trino_jdbc(dsn)
        user = parsed_url.get("user", "")
        password = parsed_url.get("password", "")
        host = parsed_url.get("host", "")
        port = int(parsed_url.get("port"))

        if not is_async:
            return create_engine(
                f"trino://{user}@{host}:{port}",
                connect_args={"http_scheme": "http"},
                future=True,
                echo=False,
            )

        return create_async_engine(
            f"trino://{user}@{host}:{port}",
            connect_args={"http_scheme": "http"},
            future=True,
            echo=False,
        )

    if not is_async:
        return create_engine(dsn, future=True, echo=False)

    return create_async_engine(dsn, future=True, echo=False)
