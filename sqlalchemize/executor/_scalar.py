from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemize.executor._abstract import AbstractExecutor, BindArgs, ExecOpts, Params, T

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy.sql.selectable import TypedReturnsRows



class ScalarExecutor(AbstractExecutor):
    """
    Executor wrapper that returns scalar values instead of row objects.

    This is a typed helper around `AsyncSession.scalars` / `scalar` and
    is intended for SELECTs like `select(User.id)` or `statement.returning(...)`.
    """

    __slots__ = ()

    async def execute(
        self,
        executable: TypedReturnsRows[tuple[T]],
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> T | None:
        """
        Execute a statement and return a single scalar value or None.

        This mirrors the behavior of `AsyncSession.scalar`, which returns
        the first column of the first row, or None if no row is present.
        """
        return await self._session.scalar(
            executable,
            params=params,
            execution_options=execution_options or {},
            bind_arguments=bind_arguments,
        )

    async def one(
        self,
        executable: TypedReturnsRows[tuple[T]],
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> T:
        """
        Executes a statement and return exactly one scalar value.

        Equivalent to `AsyncSession.scalars(...).one()`, so it raises:

        Raises
        ------
        sqlalchemy.exc.NoResultFound
        sqlalchemy.exc.MultipleResultsFound
        """
        result = await self._session.scalars(
            executable,
            params=params,
            execution_options=execution_options or {},
            bind_arguments=bind_arguments,
        )
        return result.one()

    async def one_or_none(
        self,
        executable: TypedReturnsRows[tuple[T]],
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> T | None:
        """
        Executes a statement and return a single scalar value or None.

        Equivalent to `AsyncSession.scalars(...).one_or_none()`.
        """
        result = await self._session.scalars(
            executable,
            params=params,
            execution_options=execution_options or {},
            bind_arguments=bind_arguments,
        )
        return result.one_or_none()

    async def first(
        self,
        executable: TypedReturnsRows[tuple[T]],
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> T | None:
        """
        Execute a statement and return the first scalar value or None.

        Equivalent to `AsyncSession.scalars(...).first()`.
        """
        result = await self._session.scalars(
            executable,
            params=params,
            execution_options=execution_options or {},
            bind_arguments=bind_arguments,
        )
        return result.first()

    async def all(
        self,
        executable: TypedReturnsRows[tuple[T]],
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> list[T]:
        """
        Executes a statement and return all scalar values as a list.
        Equivalent to `await session.scalars(...).all()`.
        """
        result = await self._session.scalars(
            executable,
            params=params,
            execution_options=execution_options or {},
            bind_arguments=bind_arguments,
        )
        items = result.all()
        return list(items)

    async def stream(
        self,
        executable: TypedReturnsRows[tuple[T]],
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> AsyncIterator[T]:
        """
        Execute a statement and yield scalar values in an async iterator.

        Example
        -------
        ```python
        async for value in executor.stream(select(User.id)):
            print(value)
        ```
        """
        result = await self._session.stream_scalars(
            executable,
            params=params,
            execution_options=execution_options or {},
            bind_arguments=bind_arguments,
        )
        async for item in result:
            yield item
