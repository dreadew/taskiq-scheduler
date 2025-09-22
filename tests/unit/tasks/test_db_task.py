import uuid
from unittest.mock import patch

import pytest

from src.infra.tasks import db_task


@pytest.mark.unit
def test_run_task_success():
    execution_id = uuid.uuid4()

    with (
        patch("src.infra.tasks.db_task._save_task_startup") as mock_startup,
        patch("src.infra.tasks.db_task._apply_ddl_and_queries") as mock_queries,
        patch("src.infra.tasks.db_task._save_task_result") as mock_result,
    ):

        mock_result.return_value = None
        mock_startup.return_value = None
        mock_queries.return_value = []

        db_task._run_task(execution_id, "dsn", ["DDL"], ["QUERY"])

        mock_startup.assert_called_once_with(execution_id)
        mock_queries.assert_called_once_with(execution_id, "dsn", ["DDL"], ["QUERY"])
        mock_result.assert_called_once_with(execution_id, [])
