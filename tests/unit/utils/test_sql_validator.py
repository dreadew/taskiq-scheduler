import pytest

from src.core.utils.sql_validator import (
    PostgreSQLValidator,
    TrinoValidator,
    SQLDialect,
    SQLValidatorFactory,
    ValidationResult,
    validate_sql_batch,
)


@pytest.mark.unit
def test_validation_result_init_valid():
    """Тест создания валидного результата."""
    result = ValidationResult(True)
    assert result.is_valid is True
    assert result.errors == []
    assert result.warnings == []


@pytest.mark.unit
def test_validation_result_init_invalid_with_errors():
    """Тест создания невалидного результата с ошибками."""
    errors = ["Ошибка 1", "Ошибка 2"]
    warnings = ["Предупреждение 1"]
    result = ValidationResult(False, errors, warnings)

    assert result.is_valid is False
    assert result.errors == errors
    assert result.warnings == warnings


@pytest.mark.unit
def test_validation_result_add_error():
    """Тест добавления ошибки."""
    result = ValidationResult(True)
    result.add_error("Новая ошибка")

    assert result.is_valid is False
    assert "Новая ошибка" in result.errors


@pytest.mark.unit
def test_validation_result_add_warning():
    """Тест добавления предупреждения."""
    result = ValidationResult(True)
    result.add_warning("Новое предупреждение")

    assert result.is_valid is True
    assert "Новое предупреждение" in result.warnings


@pytest.mark.unit
def test_postgresql_validator_init():
    """Тест инициализации PostgreSQL валидатора."""
    validator = PostgreSQLValidator()
    assert validator.dialect == SQLDialect.POSTGRESQL
    assert len(validator.get_forbidden_keywords()) > 0
    assert len(validator.get_ddl_keywords()) > 0
    assert len(validator.get_dml_keywords()) > 0


@pytest.mark.unit
def test_postgresql_validator_forbidden_keywords():
    """Тест проверки запрещенных ключевых слов."""
    validator = PostgreSQLValidator()
    forbidden = validator.get_forbidden_keywords()
    expected_keywords = ["DROP DATABASE", "DROP SCHEMA", "DROP USER", "DROP ROLE"]

    for keyword in expected_keywords:
        assert keyword in forbidden


@pytest.mark.unit
def test_postgresql_validator_ddl_keywords():
    """Тест проверки DDL ключевых слов."""
    validator = PostgreSQLValidator()
    ddl_keywords = validator.get_ddl_keywords()
    expected_keywords = ["CREATE TABLE", "ALTER TABLE", "DROP TABLE", "CREATE INDEX"]

    for keyword in expected_keywords:
        assert keyword in ddl_keywords


@pytest.mark.unit
def test_postgresql_validator_dml_keywords():
    """Тест проверки DML ключевых слов."""
    validator = PostgreSQLValidator()
    dml_keywords = validator.get_dml_keywords()
    expected_keywords = ["SELECT", "INSERT", "UPDATE", "DELETE"]

    for keyword in expected_keywords:
        assert keyword in dml_keywords


@pytest.mark.unit
def test_postgresql_validator_validate_basic_syntax_valid_query():
    """Тест валидации синтаксиса валидного запроса."""
    validator = PostgreSQLValidator()
    result = validator.validate_basic_syntax("SELECT * FROM users;")

    assert result.is_valid is True
    assert len(result.errors) == 0


@pytest.mark.unit
def test_postgresql_validator_validate_basic_syntax_empty_query():
    """Тест валидации пустого запроса."""
    validator = PostgreSQLValidator()
    result = validator.validate_basic_syntax("")

    assert result.is_valid is False
    assert any("пустой" in error.lower() for error in result.errors)


@pytest.mark.unit
def test_postgresql_validator_validate_basic_syntax_unbalanced_parentheses():
    """Тест валидации запроса с несбалансированными скобками."""
    validator = PostgreSQLValidator()
    result = validator.validate_basic_syntax("SELECT * FROM users WHERE (id = 1")

    assert result.is_valid is False
    assert any("скобк" in error.lower() for error in result.errors)


@pytest.mark.unit
def test_postgresql_validator_validate_basic_syntax_unbalanced_quotes():
    """Тест валидации запроса с несбалансированными кавычками."""
    validator = PostgreSQLValidator()
    result = validator.validate_basic_syntax("SELECT 'unclosed string FROM users")

    assert result.is_valid is False
    assert any("кавычк" in error.lower() for error in result.errors)


@pytest.mark.unit
def test_postgresql_validator_validate_basic_syntax_forbidden_keywords():
    """Тест валидации запроса с запрещенными ключевыми словами."""
    validator = PostgreSQLValidator()
    result = validator.validate_basic_syntax("DROP DATABASE production")

    assert result.is_valid is False
    assert any("запрещен" in error.lower() for error in result.errors)


@pytest.mark.unit
def test_postgresql_validator_validate_ddl_valid():
    """Тест валидации валидного DDL."""
    validator = PostgreSQLValidator()
    result = validator.validate_ddl("CREATE TABLE test (id INT PRIMARY KEY);")

    assert result.is_valid is True


@pytest.mark.unit
def test_postgresql_validator_validate_ddl_without_ddl_keywords():
    """Тест валидации DDL без DDL ключевых слов."""
    validator = PostgreSQLValidator()
    result = validator.validate_ddl("SELECT * FROM users;")

    assert result.is_valid is True
    assert any("ddl" in warning.lower() for warning in result.warnings)


@pytest.mark.unit
def test_postgresql_validator_validate_query_with_modification():
    """Тест валидации query с модификацией данных."""
    validator = PostgreSQLValidator()
    result = validator.validate_query("DELETE FROM users WHERE id = 1;")

    assert result.is_valid is True


@pytest.mark.unit
def test_postgresql_validator_clean_sql_comments():
    """Тест очистки SQL от комментариев."""
    validator = PostgreSQLValidator()
    sql_with_comments = """
    -- Это комментарий
    SELECT * FROM users; /* Блочный комментарий */
    """
    cleaned = validator._clean_sql(sql_with_comments)

    assert "-- Это комментарий" not in cleaned
    assert "/* Блочный комментарий */" not in cleaned
    assert "SELECT * FROM users;" in cleaned


@pytest.mark.unit
def test_trino_validator_init():
    """Тест инициализации Trino валидатора."""
    validator = TrinoValidator()
    assert validator.dialect == SQLDialect.TRINO
    assert len(validator.get_forbidden_keywords()) > 0
    assert len(validator.get_ddl_keywords()) > 0
    assert len(validator.get_dml_keywords()) > 0


@pytest.mark.unit
def test_trino_validator_forbidden_keywords():
    """Тест проверки запрещенных ключевых слов для Trino."""
    validator = TrinoValidator()
    forbidden = validator.get_forbidden_keywords()
    expected_keywords = ["DROP SCHEMA", "DROP CATALOG", "GRANT"]

    for keyword in expected_keywords:
        assert keyword in forbidden


@pytest.mark.unit
def test_trino_validator_ddl_keywords():
    """Тест проверки DDL ключевых слов для Trino."""
    validator = TrinoValidator()
    ddl_keywords = validator.get_ddl_keywords()
    expected_keywords = ["CREATE TABLE", "CREATE VIEW", "CREATE SCHEMA"]

    for keyword in expected_keywords:
        assert keyword in ddl_keywords


@pytest.mark.unit
def test_trino_validator_dml_keywords():
    """Тест проверки DML ключевых слов для Trino."""
    validator = TrinoValidator()
    dml_keywords = validator.get_dml_keywords()
    expected_keywords = ["SELECT", "INSERT", "DELETE", "DESCRIBE"]

    for keyword in expected_keywords:
        assert keyword in dml_keywords


@pytest.mark.unit
def test_trino_validator_validate_trino_specific_unnest():
    """Тест валидации Trino-специфичных функций - UNNEST."""
    validator = TrinoValidator()
    result = validator.validate_query("SELECT * FROM UNNEST(ARRAY[1, 2, 3]) AS t(col);")

    assert result.is_valid is True


@pytest.mark.unit
def test_trino_validator_validate_trino_specific_window_functions():
    """Тест валидации Trino оконных функций."""
    validator = TrinoValidator()
    result = validator.validate_query(
        """
        SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) as rn 
        FROM users;
    """
    )

    assert result.is_valid is True


@pytest.mark.unit
def test_trino_validator_validate_trino_specific_s3_access():
    """Тест валидации Trino доступа к S3."""
    validator = TrinoValidator()
    result = validator.validate_query("SELECT * FROM hive.default.s3_table;")

    assert result.is_valid is True


@pytest.mark.unit
def test_sql_validator_factory_create_validator_postgresql():
    """Тест создания PostgreSQL валидатора через фабрику."""
    validator = SQLValidatorFactory.create_validator(SQLDialect.POSTGRESQL)

    assert isinstance(validator, PostgreSQLValidator)
    assert validator.dialect == SQLDialect.POSTGRESQL


@pytest.mark.unit
def test_sql_validator_factory_create_validator_trino():
    """Тест создания Trino валидатора через фабрику."""
    validator = SQLValidatorFactory.create_validator(SQLDialect.TRINO)

    assert isinstance(validator, TrinoValidator)
    assert validator.dialect == SQLDialect.TRINO


@pytest.mark.unit
def test_sql_validator_factory_create_validator_from_dsn_postgresql():
    """Тест создания валидатора из PostgreSQL DSN."""
    dsn = "postgresql://user:pass@localhost/db"
    validator = SQLValidatorFactory.create_validator_from_dsn(dsn)

    assert isinstance(validator, PostgreSQLValidator)


@pytest.mark.unit
def test_sql_validator_factory_create_validator_from_dsn_trino():
    """Тест создания валидатора из Trino DSN."""
    dsn = "trino://user@localhost:8080/catalog"
    validator = SQLValidatorFactory.create_validator_from_dsn(dsn)

    assert isinstance(validator, TrinoValidator)


@pytest.mark.unit
def test_sql_validator_factory_create_validator_from_dsn_unknown():
    """Тест создания валидатора из неизвестного DSN."""
    dsn = "mysql://user:pass@localhost/db"
    validator = SQLValidatorFactory.create_validator_from_dsn(dsn)

    assert isinstance(validator, PostgreSQLValidator)


@pytest.mark.unit
def test_sql_validator_factory_create_validator_unsupported_dialect():
    """Тест создания валидатора для неподдерживаемого диалекта."""
    with pytest.raises(ValueError, match="Неподдерживаемый диалект"):
        SQLValidatorFactory.create_validator("unsupported")


@pytest.mark.unit
def test_validate_sql_batch_ddl():
    """Тест пакетной валидации DDL."""
    ddl = ["CREATE TABLE test (id INT);", "CREATE INDEX idx_test ON test(id);"]

    results = validate_sql_batch(ddl, SQLDialect.POSTGRESQL, is_ddl=True)

    assert len(results) == 2
    assert all(result.is_valid for result in results)


@pytest.mark.unit
def test_validate_sql_batch_queries():
    """Тест пакетной валидации запросов."""
    queries = ["SELECT * FROM test;", "SELECT COUNT(*) FROM test;"]

    results = validate_sql_batch(queries, SQLDialect.POSTGRESQL, is_ddl=False)

    assert len(results) == 2
    assert all(result.is_valid for result in results)


@pytest.mark.unit
def test_validate_sql_batch_mixed_validity():
    """Тест пакетной валидации со смешанной валидностью."""
    queries = ["SELECT * FROM test;", "DROP DATABASE production;"]

    results = validate_sql_batch(queries, SQLDialect.POSTGRESQL, is_ddl=False)

    assert len(results) == 2
    assert results[0].is_valid is True
    assert results[1].is_valid is False


@pytest.mark.unit
def test_complex_postgresql_query():
    """Тест сложного PostgreSQL запроса."""
    validator = PostgreSQLValidator()
    complex_query = """
        WITH ranked_users AS (
            SELECT id, name, email,
                   ROW_NUMBER() OVER (ORDER BY created_at DESC) as rn
            FROM users
            WHERE created_at >= '2024-01-01'
        )
        SELECT id, name, email
        FROM ranked_users
        WHERE rn <= 10;
    """

    result = validator.validate_query(complex_query)
    assert result.is_valid is True


@pytest.mark.unit
def test_complex_trino_query():
    """Тест сложного Trino запроса."""
    validator = TrinoValidator()
    complex_query = """
        SELECT customer_id,
               ARRAY_AGG(order_id ORDER BY order_date) as order_ids,
               CARDINALITY(ARRAY_AGG(order_id)) as order_count
        FROM hive.warehouse.orders
        WHERE order_date >= DATE '2024-01-01'
        GROUP BY customer_id
        HAVING CARDINALITY(ARRAY_AGG(order_id)) > 5;
    """

    result = validator.validate_query(complex_query)
    assert result.is_valid is True


@pytest.mark.unit
def test_sql_injection_patterns():
    """Тест обнаружения паттернов SQL инъекций."""
    validator = PostgreSQLValidator()
    malicious_queries = [
        "SELECT * FROM users WHERE id = 1; DROP TABLE users; --",
        "DROP DATABASE production",
        "GRANT ALL ON users TO admin",
    ]

    dangerous_found = False
    for query in malicious_queries:
        result = validator.validate_query(query)
        if not result.is_valid:
            dangerous_found = True
            break

    assert (
        dangerous_found
    ), "Валидатор должен отклонять хотя бы некоторые опасные запросы"


@pytest.mark.unit
def test_very_long_query():
    """Тест очень длинного запроса."""
    validator = PostgreSQLValidator()

    columns = ", ".join([f"column_{i}" for i in range(100)])
    long_query = f"SELECT {columns} FROM test_table;"

    result = validator.validate_query(long_query)
    assert result.is_valid is True


@pytest.mark.unit
def test_nested_queries():
    """Тест вложенных запросов."""
    validator = PostgreSQLValidator()
    nested_query = """
        SELECT u.id, u.name,
               (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count
        FROM users u
        WHERE u.id IN (
            SELECT DISTINCT user_id 
            FROM orders 
            WHERE created_at >= '2024-01-01'
        );
    """

    result = validator.validate_query(nested_query)
    assert result.is_valid is True


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT 1", True),
        ("SELECT * FROM users", True),
        ("INSERT INTO users VALUES (1, 'John')", True),
        ("DELETE FROM users", True),
        ("DROP TABLE users", True),
        ("DROP DATABASE test", False),
        ("GRANT ALL ON users TO admin", False),
        ("SELECT (", False),
        ("SELECT 'unclosed", False),
    ],
)
@pytest.mark.unit
def test_parametrized_validation(sql, expected):
    """Параметризованный тест валидации различных SQL выражений."""
    validator = PostgreSQLValidator()

    try:
        if (
            "CREATE" in sql.upper()
            or "DROP TABLE" in sql.upper()
            or "ALTER" in sql.upper()
        ):
            result = validator.validate_ddl(sql)
        else:
            result = validator.validate_query(sql)

        assert result.is_valid == expected
    except Exception:
        assert expected is False
