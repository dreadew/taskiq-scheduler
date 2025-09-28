import uuid
from unittest.mock import AsyncMock, patch

import pytest

from src.infra.tasks import db_task


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.xfail
async def test_run_task_success():
    execution_id = uuid.uuid4()
    mock_ctx = AsyncMock()
    mock_ctx.is_cancelled.return_value = False

    mock_grpc_response = {
        "success": True,
        "message": "Analysis completed",
        "ddl": [{"statement": "CREATE TABLE test (id INT)"}],
        "migrations": [{"statement": "ALTER TABLE test ADD COLUMN name VARCHAR(50)"}],
        "queries": [{"query_id": "q1", "query": "SELECT * FROM test"}],
        "warnings": ["Warning: table has no primary key"],
    }

    with (
        patch("src.infra.tasks.db_task._save_task_startup") as mock_startup,
        patch("src.infra.tasks.db_task._apply_ddl_and_queries") as mock_queries,
        patch("src.infra.tasks.db_task._save_task_result") as mock_result,
    ):

        mock_result.return_value = None
        mock_startup.return_value = None
        mock_queries.return_value = mock_grpc_response

        await db_task._run_task(execution_id, "dsn", ["DDL"], ["QUERY"], mock_ctx)

        mock_startup.assert_called_once_with(execution_id)
        mock_queries.assert_called_once_with(
            execution_id, "dsn", ["DDL"], ["QUERY"], mock_ctx
        )
        mock_result.assert_called_once_with(execution_id, mock_grpc_response)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_apply_ddl_and_queries_grpc_integration():
    """Тест интеграции с gRPC клиентом"""
    execution_id = uuid.uuid4()
    mock_ctx = AsyncMock()
    mock_ctx.is_cancelled.return_value = False

    ddl_data = [{"statement": "CREATE TABLE users (id INT PRIMARY KEY)"}]
    queries_data = [
        {
            "queryid": "q1",
            "query": "SELECT COUNT(*) FROM users",
            "runquantity": 10,
            "executiontime": 100,
        }
    ]

    mock_grpc_response = {
        "success": True,
        "message": "Schema analysis completed successfully",
        "ddl": [{"statement": "CREATE TABLE users (id INT PRIMARY KEY)"}],
        "migrations": [{"statement": "CREATE INDEX idx_users_id ON users(id)"}],
        "queries": [{"query_id": "q1", "query": "SELECT COUNT(*) FROM users"}],
        "warnings": [],
    }

    with patch("src.infra.clients.grpc_client.schema_review_client") as mock_client:
        mock_client.__aenter__.return_value = mock_client
        mock_client.review_schema.return_value = mock_grpc_response

        result = await db_task._apply_ddl_and_queries(
            execution_id, "postgresql://test", ddl_data, queries_data, mock_ctx
        )

        mock_client.review_schema.assert_called_once_with(
            url="postgresql://test",
            ddl_statements=["CREATE TABLE users (id INT PRIMARY KEY)"],
            queries=[
                {
                    "query_id": "q1",
                    "query": "SELECT COUNT(*) FROM users",
                    "runquantity": 10,
                    "executiontime": 100,
                }
            ],
            thread_id=str(execution_id),
        )

        assert result == mock_grpc_response
        assert "ddl" in result
        assert "migrations" in result
        assert "queries" in result
