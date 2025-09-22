import pytest

from src.core.utils.parse_url import get_db_type, parse_trino_jdbc


@pytest.mark.parametrize(
    "url, expected",
    [
        ("jdbc:trino://192.168.0.1:8090?user=admin&password=admin", "trino"),
        ("jdbc:postgresql://192.168.0.1:8090?user=admin&password=admin", "postgresql"),
    ],
)
@pytest.mark.unit
def test_utils_get_db_type(url, expected):
    """
    Тест функции для определения типа БД
    """
    assert get_db_type(url) == expected


@pytest.mark.unit
def test_utils_parse_trino_jdbc():
    """
    Тест функции для парсинга jdbc строки trino
    """

    expected = {
        "host": "192.168.0.1",
        "port": 8090,
        "user": "admin",
        "password": "admin",
    }
    url = f"jdbc:trino://{expected['host']}:{expected['port']}?user={expected['user']}&password={expected['password']}"

    parsed_url = parse_trino_jdbc(url)

    assert parsed_url == expected


@pytest.mark.unit
def test_utils_get_db_type_invalid_format():
    """
    Тест функции парсинга типа БД с некорректной строкой подключения
    """

    with pytest.raises(ValueError):
        get_db_type("trino//host")


@pytest.mark.unit
def test_utils_parse_trino_jdbc_missing_parts():
    """
    Тест функции парсинга jdbc строки trino без переданного user и password
    """

    with pytest.raises(ValueError):
        parse_trino_jdbc("jdbc:trino://192.168.0.1:8090")
