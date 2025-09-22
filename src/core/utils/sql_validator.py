import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Set

from src.core.logging import get_logger

logger = get_logger(__name__)


class SQLDialect(Enum):
    """Поддерживаемые SQL диалекты."""

    POSTGRESQL = "postgresql"
    TRINO = "trino"


class ValidationResult:
    """Результат валидации SQL."""

    def __init__(
        self, is_valid: bool, errors: List[str] = None, warnings: List[str] = None
    ):
        self.is_valid = is_valid
        self.errors = errors or []
        self.warnings = warnings or []

    def add_error(self, error: str):
        """Добавить ошибку."""
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str):
        """Добавить предупреждение."""
        self.warnings.append(warning)


class BaseSQLValidator(ABC):
    """Базовый класс для валидаторов SQL."""

    def __init__(self, dialect: SQLDialect):
        self.dialect = dialect

    @abstractmethod
    def get_forbidden_keywords(self) -> Set[str]:
        """Получить список запрещенных ключевых слов."""
        pass

    @abstractmethod
    def get_ddl_keywords(self) -> Set[str]:
        """Получить список DDL ключевых слов."""
        pass

    @abstractmethod
    def get_dml_keywords(self) -> Set[str]:
        """Получить список DML ключевых слов."""
        pass

    def validate_basic_syntax(self, sql: str) -> ValidationResult:
        """Базовая валидация синтаксиса SQL."""
        result = ValidationResult(True)

        cleaned_sql = self._clean_sql(sql)

        if not cleaned_sql.strip():
            result.add_error("SQL запрос пустой")
            return result

        self._validate_parentheses(cleaned_sql, result)
        self._validate_quotes(cleaned_sql, result)
        self._validate_forbidden_keywords(cleaned_sql, result)

        return result

    def validate_ddl(self, sql: str) -> ValidationResult:
        """Валидация DDL запросов."""
        result = self.validate_basic_syntax(sql)

        if not result.is_valid:
            return result

        cleaned_sql = self._clean_sql(sql).upper()
        ddl_keywords = self.get_ddl_keywords()

        if not any(keyword in cleaned_sql for keyword in ddl_keywords):
            result.add_warning("Запрос не содержит DDL ключевых слов")

        return result

    def validate_query(self, sql: str) -> ValidationResult:
        """Валидация DML запросов."""
        result = self.validate_basic_syntax(sql)

        if not result.is_valid:
            return result

        cleaned_sql = self._clean_sql(sql).upper()

        if any(
            keyword in cleaned_sql
            for keyword in ["DELETE", "UPDATE", "INSERT", "TRUNCATE"]
        ):
            result.add_warning("Запрос содержит операции изменения данных")

        return result

    def _clean_sql(self, sql: str) -> str:
        """Очистка SQL от комментариев и лишних пробелов."""
        sql = re.sub(r"--.*$", "", sql, flags=re.MULTILINE)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        sql = re.sub(r"\s+", " ", sql).strip()

        return sql

    def _validate_parentheses(self, sql: str, result: ValidationResult):
        """Проверка сбалансированности скобок."""
        stack = []
        for char in sql:
            if char == "(":
                stack.append(char)
            elif char == ")":
                if not stack:
                    result.add_error(
                        "Несбалансированные скобки: лишняя закрывающая скобка"
                    )
                    return
                stack.pop()

        if stack:
            result.add_error("Несбалансированные скобки: незакрытые скобки")

    def _validate_quotes(self, sql: str, result: ValidationResult):
        """Проверка корректности кавычек."""
        single_quote_count = sql.count("'")
        double_quote_count = sql.count('"')

        if single_quote_count % 2 != 0:
            result.add_error("Несбалансированные одинарные кавычки")

        if double_quote_count % 2 != 0:
            result.add_error("Несбалансированные двойные кавычки")

    def _validate_forbidden_keywords(self, sql: str, result: ValidationResult):
        """Проверка на запрещенные ключевые слова."""
        cleaned_sql = self._clean_sql(sql).upper()
        forbidden = self.get_forbidden_keywords()

        for keyword in forbidden:
            if keyword in cleaned_sql:
                result.add_error(f"Запрещенное ключевое слово: {keyword}")


class PostgreSQLValidator(BaseSQLValidator):
    """Валидатор для PostgreSQL."""

    def __init__(self):
        super().__init__(SQLDialect.POSTGRESQL)

    def get_forbidden_keywords(self) -> Set[str]:
        """Запрещенные операации для PostgreSQL."""
        return {
            "DROP DATABASE",
            "DROP SCHEMA",
            "DROP USER",
            "DROP ROLE",
            "ALTER USER",
            "ALTER ROLE",
            "GRANT",
            "REVOKE",
            "VACUUM",
            "REINDEX",
            "CLUSTER",
        }

    def get_ddl_keywords(self) -> Set[str]:
        """DDL ключевые слова PostgreSQL."""
        return {
            "CREATE TABLE",
            "CREATE INDEX",
            "CREATE VIEW",
            "ALTER TABLE",
            "DROP TABLE",
            "DROP INDEX",
            "DROP VIEW",
        }

    def get_dml_keywords(self) -> Set[str]:
        """DML ключевые слова PostgreSQL."""
        return {
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "WITH",
            "UNION",
            "INTERSECT",
            "EXCEPT",
        }


class TrinoValidator(BaseSQLValidator):
    """Валидатор для Trino/Presto."""

    def __init__(self):
        super().__init__(SQLDialect.TRINO)

    def get_forbidden_keywords(self) -> Set[str]:
        """Запрещенные операации для Trino."""
        return {
            "DROP SCHEMA",
            "DROP CATALOG",
            "DROP USER",
            "DROP ROLE",
            "ALTER SCHEMA",
            "ALTER CATALOG",
            "GRANT",
            "REVOKE",
            "CREATE USER",
            "CREATE ROLE",
        }

    def get_ddl_keywords(self) -> Set[str]:
        """DDL ключевые слова Trino."""
        return {
            "CREATE TABLE",
            "CREATE VIEW",
            "CREATE SCHEMA",
            "DROP TABLE",
            "DROP VIEW",
            "ALTER TABLE",
        }

    def get_dml_keywords(self) -> Set[str]:
        """DML ключевые слова Trino."""
        return {
            "SELECT",
            "INSERT",
            "DELETE",
            "WITH",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "DESCRIBE",
            "SHOW TABLES",
            "SHOW SCHEMAS",
        }

    def validate_trino_specific(self, sql: str) -> ValidationResult:
        """Специфичная для Trino валидация."""
        result = self.validate_basic_syntax(sql)

        if not result.is_valid:
            return result

        cleaned_sql = self._clean_sql(sql).upper()

        if "UNNEST" in cleaned_sql:
            result.add_warning("Использование UNNEST - проверьте совместимость")

        if "ROW_NUMBER() OVER" in cleaned_sql:
            result.add_warning("Использование оконных функций - может быть медленным")

        if any(keyword in cleaned_sql for keyword in ["S3://", "S3A://", "S3N://"]):
            result.add_warning(
                "Запрос работает с S3 данными - учтите возможные задержки"
            )

        return result


class SQLValidatorFactory:
    """Фабрика для создания валидаторов SQL."""

    @staticmethod
    def create_validator(dialect: SQLDialect) -> BaseSQLValidator:
        """Создать валидатор для указанного диалекта."""
        if dialect == SQLDialect.POSTGRESQL:
            return PostgreSQLValidator()
        elif dialect == SQLDialect.TRINO:
            return TrinoValidator()
        else:
            raise ValueError(f"неподдерживаемый диалект: {dialect}.")

    @staticmethod
    def create_validator_from_dsn(dsn: str) -> BaseSQLValidator:
        """Создать валидатор на основе DSN."""
        from src.core.utils.parse_url import get_db_type

        db_type = get_db_type(dsn).lower()

        if "postgresql" in db_type:
            return PostgreSQLValidator()
        elif db_type == "trino":
            return TrinoValidator()
        else:
            logger.warning(
                f"неизвестный тип БД: {db_type}, используем PostgreSQL валидатор."
            )
            return PostgreSQLValidator()


def validate_sql_batch(
    sqls: List[str], dialect: SQLDialect, is_ddl: bool = False
) -> List[ValidationResult]:
    """
    Валидация пакета SQL запросов.

    :param sqls: список SQL запросов
    :param dialect: диалект SQL
    :param is_ddl: True если это DDL запросы
    :return: список результатов валидации
    """
    validator = SQLValidatorFactory.create_validator(dialect)
    results = []

    for sql in sqls:
        if is_ddl:
            result = validator.validate_ddl(sql)
        else:
            result = validator.validate_query(sql)
        results.append(result)

    return results
