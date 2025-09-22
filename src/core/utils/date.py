from datetime import datetime, timezone


def utc_now():
    """
    Получить DateTime с UTC временем
    :return: DateTime с UTC временем
    """
    return datetime.now(timezone.utc)
