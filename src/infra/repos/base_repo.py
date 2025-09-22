from contextlib import asynccontextmanager
from typing import Awaitable, Callable, TypeVar
from uuid import UUID

from sqlalchemy import desc
from sqlalchemy.future import select

from src.core.abstractions.repo import Repository
from src.core.models.paging import PagingParams
from src.infra.db.sqlalchemy.models.base import BaseEntity

T = TypeVar("T", bound=BaseEntity)


class BaseRepository(Repository[T]):
    """
    Базовый репозиторий
    """

    def __init__(self, entity_type: type[T], session_factory: Callable[[], Awaitable]):
        self.entity_type = entity_type
        self.session_factory = session_factory

    async def create(self, obj_in: T) -> UUID:
        async with self.session_factory() as session:
            session.add(obj_in)
            await session.commit()
            await session.refresh(obj_in)
            return obj_in.id

    async def update(self, obj_id: UUID, data: dict):
        async with self.session_factory() as session:
            obj = await session.get(self.entity_type, obj_id)
            if obj is None:
                raise ValueError(f"объект с id: {obj_id} не найден")
            for field, value in data.items():
                if hasattr(obj, field):
                    setattr(obj, field, value)
            await session.commit()

    async def delete(self, obj_id: UUID):
        async with self.session_factory() as session:
            obj = await session.get(self.entity_type, obj_id)
            if obj is None:
                raise ValueError(f"объект с id: {obj_id} не найден")
            await session.delete(obj)
            await session.commit()

    async def get(self, obj_id: UUID) -> T | None:
        async with self.session_factory() as session:
            return await session.get(self.entity_type, obj_id)

    async def get_for_update(self, obj_id: UUID) -> T | None:
        """Получить объект с блокировкой для обновления"""
        async with self.session_factory() as session:
            stmt = (
                select(self.entity_type)
                .where(self.entity_type.id == obj_id)
                .with_for_update()
            )
            result = await session.execute(stmt)
            return result.scalars().first()

    @asynccontextmanager
    async def transaction(self):
        """Контекстный менеджер для транзакций"""
        async with self.session_factory() as session:
            async with session.begin():
                original_factory = self.session_factory
                self.session_factory = lambda: session
                try:
                    yield session
                finally:
                    self.session_factory = original_factory

    async def get_all(self, params: PagingParams) -> list[T]:
        async with self.session_factory() as session:
            stmt = select(self.entity_type).offset(params.offset).limit(params.limit)
            result = await session.execute(stmt)
            return result.scalars().all()

    async def find_latest_by_field(self, field_name: str, field_value) -> T | None:
        """
        Найти последнюю запись по указанному полю, отсортированную по created_at DESC
        :param field_name: имя поля для поиска
        :param field_value: значение поля
        :return: последняя запись или None
        """
        async with self.session_factory() as session:
            field = getattr(self.entity_type, field_name)
            stmt = (
                select(self.entity_type)
                .where(field == field_value)
                .order_by(desc(self.entity_type.created_at))
                .limit(1)
            )
            result = await session.execute(stmt)
            return result.scalars().first()
