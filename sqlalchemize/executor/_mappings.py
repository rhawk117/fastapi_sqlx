from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemize.executor._abstract import (
    AbstractExecutor,
    BindArgs,
    ExecOpts,
    Params,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from sqlalchemy import Executable, MappingResult


class MappingsExecutor(AbstractExecutor):
    """
    Executor wrapper that returns results as plain dictionaries.

    This is a thin convenience layer over `AsyncSession.execute` that
    consistently converts `RowMapping` values into `dict` instances at
    the boundary for convience instead of a weird hybrid type.
    """

    async def execute(
        self,
        executable: Executable,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> MappingResult:
        """
        Execute a statement and return a `MappingResult`.

        The result still exposes full SQLAlchemy result APIs, but is already
        converted to mappings at the row level.
        """
        result = await self._session.execute(
            executable,
            params=params,
            execution_options=execution_options or {},
            bind_arguments=bind_arguments,
        )
        return result.mappings()

    async def one(
        self,
        executable: Executable,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> dict[str, Any]:
        """
        Execute a statement and return a single row as a dictionary.

        Raises
        ------
        sqlalchemy.exc.NoResultFound
        sqlalchemy.exc.MultipleResultsFound
        """
        mapping = (
            await self.execute(
                executable,
                params=params,
                execution_options=execution_options,
                bind_arguments=bind_arguments,
            )
        ).one()
        return dict(mapping)

    async def one_or_none(
        self,
        executable: Executable,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> dict[str, Any] | None:
        """
        Execute a statement and return a single row as a dictionary, or None.
        """
        mapping = (
            await self.execute(
                executable,
                params=params,
                execution_options=execution_options,
                bind_arguments=bind_arguments,
            )
        ).one_or_none()
        return dict(mapping) if mapping is not None else None

    async def first(
        self,
        executable: Executable,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> dict[str, Any] | None:
        """
        Execute a statement and return the first row as a dictionary, or None.
        """
        mapping = (
            await self.execute(
                executable,
                params=params,
                execution_options=execution_options,
                bind_arguments=bind_arguments,
            )
        ).first()
        return dict(mapping) if mapping is not None else None

    async def all(
        self,
        executable: Executable,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute a statement and return all rows as dictionaries.
        """
        items = (
            await self.execute(
                executable,
                params=params,
                execution_options=execution_options,
                bind_arguments=bind_arguments,
            )
        ).all()
        return list(map(dict, items))

    async def stream(
        self,
        executable: Executable,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Execute a statement and yield rows as dictionaries in an async iterator.

        Example
        -------
        ```python
        async for row in executor.stream(stmt):
            print(row['id'], row['name'])
        ```
        """
        result = await self._session.stream(
            executable,
            params=params,
            execution_options=execution_options or {},
            bind_arguments=bind_arguments,
        )
        async for mapping in result.mappings():
            yield dict(mapping)
