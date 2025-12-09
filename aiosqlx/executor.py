from __future__ import annotations

import abc
import contextlib
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Self, TypeVar

from sqlalchemy import Select, exists, func, select

if TYPE_CHECKING:
    from types import TracebackType

    from sqlalchemy import MappingResult
    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.sql import Executable
    from sqlalchemy.sql.selectable import TypedReturnsRows


Params = Mapping[str, Any] | Sequence[Mapping[str, Any]] | None
ExecOpts = Mapping[str, Any]
BindArgs = dict[str, Any]

T = TypeVar('T')


class AbstractExecutor(abc.ABC):
    """
    Abstract base class for small, typed helpers around an `AsyncSession`.

    Concrete subclasses specialize the result shape (e.g. mappings vs scalars)
    but share a common calling convention:

    * `executable` is a SQLAlchemy statement or selectable
    * `params`, `execution_options`, and `bind_arguments` are forwarded to
      the underlying `AsyncSession` execution methods
    """

    __slots__ = ('_session',)

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @abc.abstractmethod
    async def execute(
        self,
        executable: Any,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> Any:
        """
        Execute the given statement and return a backend-specific result object.

        Subclasses decide whether this returns a mapping result, scalar result,
        or something else more appropriate.
        """

    @abc.abstractmethod
    async def one(
        self,
        executable: Any,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> Any:
        """
        Execute the statement and return exactly one result.

        Implementations should propagate `NoResultFound` / `MultipleResultsFound`
        errors from SQLAlchemy, mirroring the `one()` semantics.
        """

    @abc.abstractmethod
    async def one_or_none(
        self,
        executable: Any,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> Any | None:
        """
        Execute the statement and return one result or `None`.

        Implementations should mirror the semantics of `one_or_none()`.
        """

    @abc.abstractmethod
    async def first(
        self,
        executable: Any,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> Any | None:
        """
        Execute the statement and return the first result or `None`.

        Implementations should mirror the semantics of `first()`.
        """

    @abc.abstractmethod
    async def all(
        self,
        executable: Any,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> list[Any]:
        """
        Execute the statement and return all results as a list.
        """

    @abc.abstractmethod
    async def stream(
        self,
        executable: Any,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> AsyncIterator[Any]:
        """
        Execute the statement and return an async iterator over the results.
        """


class MappingsExecutor(AbstractExecutor):
    """
    Executor wrapper that returns results as plain dictionaries.

    This is a thin convenience layer over `AsyncSession.execute` that
    consistently converts `RowMapping` values into `dict` instances at
    the boundary for convience instead of a weird hybrid type.
    """

    __slots__ = ()

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
        return [dict(row) for row in items]

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


@dataclass
class SQLAlchemyExecutor:
    """
    High-level façade around an `AsyncSession`.

    Exposes two typed executors:

    * `mappings`: for dictionary-shaped rows
    * `scalar`  : for scalar values

    And a few common convenience methods for persistence and simple
    utility queries (`count`, `exists`, `rowcount`, etc.).
    """

    session: AsyncSession
    mappings: MappingsExecutor = field(init=False)
    scalar: ScalarExecutor = field(init=False)

    def __post_init__(self) -> None:
        self.mappings = MappingsExecutor(self.session)
        self.scalar = ScalarExecutor(self.session)

    async def __aenter__(self) -> Self:
        """
        Enters an async context, returning `self`.

        Note: _This does not start a transaction, it just mirrors the session
        lifetime so you can reliably close it on exit._
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """
        On context exit, rollbacks on error and closes the session.
        """
        if exc is not None:
            await self.rollback_safe()
        await self.session.close()

    async def save(self, *, commit: bool = True) -> None:
        """
        Persist pending changes on the session.

        Parameters
        ----------
        commit : bool, optional
            If True (default), commit the transaction.
            If False, only flush changes to the database.
        """
        if commit:
            await self.session.commit()
        else:
            await self.session.flush()

    async def save_entity(
        self,
        entity: Any,
        *,
        commit: bool = True,
        refresh: bool = False,
    ) -> Any:
        """
        Add a single entity to the session and persist it.

        Parameters
        ----------
        entity : Any
            ORM instance to be added to the session.

        commit : bool, optional
            If True (default), commit the transaction.
            If False, only flush.

        refresh : bool, optional
            If True, refresh the entity from the database
            after persistence.

        Returns
        -------
        Any
            The same entity instance, potentially refreshed.
        """
        self.session.add(entity)
        await self.save(commit=commit)
        if refresh:
            await self.session.refresh(entity)
        return entity

    async def save_entities(
        self,
        entities: Sequence[Any],
        *,
        commit: bool = True,
    ) -> Sequence[Any]:
        """
        Add multiple entities to the session and persist them.

        Parameters
        ----------
        entities : Sequence[Any]
            ORM instances to be added.

        commit : bool, optional
            If True (default), commit the transaction.
            If False, only flush.

        Returns
        -------
        Sequence[Any]
            The sequence of entities passed in.
        """
        self.session.add_all(entities)
        await self.save(commit=commit)
        return entities

    async def execute_rowcount(
        self,
        executable: Executable,
        *,
        params: Params = None,
        execution_options: ExecOpts | None = None,
        bind_arguments: BindArgs | None = None,
    ) -> int:
        """
        Execute a statement and return the reported row count.

        Notes
        -----
        * Some dialects and statements, `rowcount` may be `-1` to signal
          that the value is not meaningful; this method returns that raw value.
        """
        result = await self.session.execute(
            executable,
            params=params,
            execution_options=execution_options or {},
            bind_arguments=bind_arguments,
        )
        return getattr(result, 'rowcount', 0)

    async def count_query_total(self, selectable: Select[Any]) -> int:
        """
        Executes a COUNT(*) over a selectable and return the total number of rows.

        Any ORDER BY / LIMIT / OFFSET on the selectable is stripped before
        counting, by wrapping it in a subquery for performance and correctness.

        Returns
        -------
        int
            The total number of rows matching the selectable.
        """
        count_stmt = select(func.count()).select_from(
            selectable.order_by(None).limit(None).offset(None).subquery()
        )
        return await self.scalar.one(count_stmt)  # type: ignore[return-value]

    async def exists(self, selectable: Select[Any]) -> bool:
        """
        Checks if any rows exist for the given select statement

        Returns
        -------
        bool
            True if at least one row exists, False otherwise.
        """
        exists_stmt = select(exists(selectable))
        return await self.scalar.one(exists_stmt)  # type: ignore[return-value]

    async def rollback_safe(self) -> None:
        """
        Attempts to rollback the current transaction, suppressing any errors.

        Intended for defensive cleanup in error paths and async context
        manager teardown.
        """
        with contextlib.suppress(Exception):
            await self.session.rollback()

    async def close(self) -> None:
        """
        Asynchronously close the underlying session.
        """
        await self.session.close()
