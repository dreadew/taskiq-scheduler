from urllib.parse import ParseResult, parse_qs, urlparse


def get_db_type(connection_str: str) -> str:
    """
    Определить тип базы из строки подключения.
    Поддерживает обычные URI (postgresql://...) и JDBC (jdbc:trino://...)
    :param connection_str: строка подключения
    :returns: тип БД
    """
    if connection_str.startswith("jdbc:"):
        parts = connection_str.split(":")
        if len(parts) < 2:
            raise ValueError("невалидная JDBC строка подключения.")
        return parts[1].lower()

    parsed = parse_dsn(connection_str)
    return parsed.scheme.lower()


def parse_trino_jdbc(jdbc_url: str) -> dict:
    """
    Парсит JDBC URL Trino и возвращает dict с параметрами для подключения.
    Пример входа: jdbc:trino://host:443?user=foo&password=bar

    :param jdbc_url: строка подключения jdbc
    :returns:
    {
        "host": "host",
        "port": 443,
        "user": "foo",
        "password": "bar",
        **params
    }
    """
    if not jdbc_url.startswith("jdbc:trino://"):
        raise ValueError("невалидная строка Trino JDBC")

    url = jdbc_url[len("jdbc:") :]
    parsed = urlparse(url)

    if not parsed.hostname or not parsed.port:
        raise ValueError("отсутствует host или port в jdbc строке.")

    params = parse_qs(parsed.query)
    params = {k: v[0] for k, v in params.items()}

    if "user" not in params:
        raise ValueError("trino jdbc строка должна содержать user.")

    return {
        "host": parsed.hostname,
        "port": parsed.port,
        "user": params.get("user"),
        "password": params.get("password", None),
        **params,
    }


def parse_dsn(connection_str: str) -> ParseResult:
    """
    Парсинг DSN
    :param connection_str: строка подключения к БД.
    :return: результат парсинга
    """
    if not isinstance(connection_str, str) or not connection_str:
        raise ValueError("строка подключения не может быть пустой.")

    parsed = urlparse(connection_str)
    if not parsed.scheme:
        raise ValueError("невозможно определить структуру БД.")

    return parsed
