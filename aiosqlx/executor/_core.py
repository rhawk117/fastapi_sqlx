from __future__ import annotations

import contextlib
import dataclasses as dc
from typing import TYPE_CHECKING, Any, Self

from sqlalchemy import Executable, Select, exists, func, select

from aiosqlx.executor._mappings import MappingsExecutor
from aiosqlx.executor._scalar import ScalarExecutor

if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import TracebackType

    from sqlalchemy.ext.asyncio import AsyncSession

    from aiosqlx.executor._abstract import BindArgs, ExecOpts, Params


@dc.dataclass(slots=True)
class SqlalchemyExecutor:
    """
    High-level interface with library specific integrations for
    projections and convience methods / executors for common sqlalchemy
    usage patterns.

    Exposes two typed executors, you can access them via:
    * `mappings`: for dictionary-shaped rows
    * `scalar`  : for scalar values

    And a few common convenience methods for persistence and simple
    utility queries (`count`, `exists`, `rowcount`, etc.).

    The class is also an async context manager that can be used
    to automatically close the underlying session on exit if need be
    but keep in mind it does NOT start a transaction for you.
    """

    session: AsyncSession
    mappings: MappingsExecutor = dc.field(init=False)
    scalar: ScalarExecutor = dc.field(init=False)

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
