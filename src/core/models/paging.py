from dataclasses import dataclass


@dataclass
class PagingParams:
    """
    Класс с параметрами для пагинации
    """

    offset: int = 0
    limit: int = 20
