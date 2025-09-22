from datetime import datetime
from enum import Enum
from uuid import UUID


def json_serialize(obj):
    """
    Сериализация объекта в валидный json
    :param obj: объект
    :return: json
    """
    if isinstance(obj, UUID):
        return str(obj)
    elif isinstance(obj, Enum):
        return obj.value
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, list):
        return [json_serialize(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: json_serialize(v) for k, v in obj.items()}
    else:
        return obj
