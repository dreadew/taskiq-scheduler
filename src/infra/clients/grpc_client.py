"""gRPC клиент для SchemaReviewService"""

import asyncio
from typing import List, Optional

import grpc
from grpc.aio import AioRpcError

from src.core.config import config
from src.core.logging import get_logger
from src.generated import schema_review_pb2, schema_review_pb2_grpc

logger = get_logger(__name__)


class SchemaReviewClient:
    """Асинхронный gRPC клиент для анализа схем"""

    def __init__(self, server_url: str = None):
        self.server_url = server_url or config.GRPC_URL
        self._channel: Optional[grpc.aio.Channel] = None
        self._stub: Optional[schema_review_pb2_grpc.SchemaReviewServiceStub] = None

    async def __aenter__(self):
        """Инициализация подключения при входе в контекст"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие подключения при выходе из контекста"""
        await self.disconnect()

    async def connect(self):
        """Установка соединения с gRPC сервером"""
        try:
            # Простое подключение без дополнительных настроек
            self._channel = grpc.aio.insecure_channel(self.server_url)

            await asyncio.wait_for(
                self._channel.channel_ready(), timeout=config.GRPC_CONNECTION_TIMEOUT
            )

            self._stub = schema_review_pb2_grpc.SchemaReviewServiceStub(self._channel)
            logger.info(f"gRPC подключение к {self.server_url} установлено")
        except Exception as e:
            logger.error(f"Ошибка подключения к gRPC серверу: {e}")
            raise

    async def disconnect(self):
        """Закрытие соединения"""
        if self._channel:
            await self._channel.close()
            logger.info("gRPC соединение закрыто")

    async def review_schema(
        self,
        url: str,
        ddl_statements: List[str],
        queries: List[dict],
        thread_id: Optional[str] = None,
    ) -> dict:
        """
        Отправка схемы на анализ в gRPC сервис

        :param url: URL базы данных
        :param ddl_statements: Список DDL команд
        :param queries: Список запросов с метаданными
        :param thread_id: ID потока (опционально)

        :return: Результат анализа схемы
        """
        if not self._stub:
            raise RuntimeError("gRPC клиент не подключен")

        try:
            ddl_list = [
                schema_review_pb2.DDLStatement(statement=ddl) for ddl in ddl_statements
            ]

            query_list = [
                schema_review_pb2.Query(
                    query_id=query.get("query_id", ""),
                    query=query.get("query", ""),
                    runquantity=query.get("runquantity", 0),
                    executiontime=query.get("executiontime", 0),
                )
                for query in queries
            ]

            request = schema_review_pb2.ReviewSchemaRequest(
                url=url, ddl=ddl_list, queries=query_list
            )

            if thread_id:
                request.thread_id = thread_id

            logger.info(
                f"Отправка схемы на анализ: DDL={len(ddl_statements)}, Queries={len(queries)}"
            )
            response = await self._stub.ReviewSchema(
                request, timeout=config.GRPC_TIMEOUT
            )

            result = {
                "success": response.success,
                "message": response.message,
                "ddl": [{"statement": ddl.statement} for ddl in response.ddl],
                "migrations": [
                    {"statement": mig.statement} for mig in response.migrations
                ],
                "queries": [
                    {"query_id": q.query_id, "query": q.query} for q in response.queries
                ],
                "warnings": list(response.warnings),
            }

            if response.HasField("error"):
                result["error"] = response.error

            logger.info(f"Получен ответ от gRPC сервиса: success={response.success}")
            return result

        except AioRpcError as e:
            logger.error(f"gRPC ошибка: {e.code()} - {e.details()}")
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при вызове gRPC: {e}")
            raise


schema_review_client = SchemaReviewClient()
