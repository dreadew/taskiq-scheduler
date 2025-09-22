from abc import ABC, abstractmethod
from typing import Generic, List, TypeVar
from uuid import UUID

from src.core.models.paging import PagingParams

T = TypeVar("T")


class Repository(ABC, Generic[T]):
    """
    Базовый класс репозитория
    """

    @abstractmethod
    async def create(self, obj_in: T) -> UUID:
        """
        Создать запись в БД
        :param obj_in: данные для создания записи
        :return: идентификатор созданной записи
        """
        pass

    @abstractmethod
    async def update(self, obj_id: UUID, obj_in: T):
        """
        Обновить запись в БД
        :param obj_id: идентификатор обновляемой записи
        :param obj_in: данные для обновления
        """
        pass

    @abstractmethod
    async def delete(self, obj_id: UUID):
        """
        Удалить запись в БД
        :param obj_id: идентификатор удаляемой записи
        """
        pass

    @abstractmethod
    async def get(self, obj_id: UUID) -> T | None:
        """
        Получить запись по идентификатору
        :param obj_id: идентификатор объекта
        :return: запись из БД
        """
        pass

    @abstractmethod
    async def get_all(self, params: PagingParams) -> List[T]:
        """
        Получить все записи с пагинацией
        :param params: параметры пагинации
        :return: записи из БД
        """
        pass
