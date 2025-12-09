from __future__ import annotations

import contextlib
import dataclasses as dc
import warnings
from typing import TYPE_CHECKING, Any, Self

from sqlalchemy import text as sqlalchemy_text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from aiosqlx.db._mapper_utils import register_sqlalchemy_models
from aiosqlx.exceptions import DatabaseInitializationError

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator
    from types import TracebackType

    from sqlalchemy import URL
    from sqlalchemy.orm import DeclarativeBase

    from aiosqlx.db._types import DatabaseConfig



@dc.dataclass(slots=True)
class SqlalchemyDatabase:
    """
    High-level wrapper around SQLAlchemy's async engine and sessionmaker.

    This class owns:

    - an `AsyncEngine` (created via `configure()` or injected via `create()`)
    - an `async_sessionmaker[AsyncSession]`
    - convenience async context managers for connections, sessions, and transactions
    - async context manager support for the database itself
    (`async with SqlalchemyDatabase(...)`)
    """
    url: URL
    mapped_base: type[DeclarativeBase]
    _async_engine: AsyncEngine | None = dc.field(default=None, init=False)
    _async_session_maker: async_sessionmaker[AsyncSession] | None = dc.field(
        default=None,
        init=False,
    )

    @classmethod
    def create(
        cls,
        *,
        url: URL,
        mapped_base: type[DeclarativeBase],
        engine: AsyncEngine,
        sessionmaker: async_sessionmaker[AsyncSession],
    ) -> Self:
        """
        Create a `SqlalchemyDatabase` instance, optionally injecting an existing
        `AsyncEngine` and/or `async_sessionmaker`.

        This is useful when you want to own the underlying engine or sessionmaker
        lifecycle outside of `SqlalchemyDatabase`; generally it's reccomended to use the
        alternative `configure()` method to set up the engine and sessionmaker so you
        don't manage the resource lifecycle yourself.
        """
        this = cls(url=url, mapped_base=mapped_base)
        this._async_engine = engine
        this._async_session_maker = sessionmaker
        return this


    @property
    def async_engine(self) -> AsyncEngine:
        """
        Public getter for the async engine.

        Returns
        -------
        AsyncEngine
            The initialized async engine.

        Raises
        ------
        DatabaseInitializationError
            If the engine has not been initialized.
        """
        if self._async_engine is None:
            raise DatabaseInitializationError(
                'The database was never initialized',
            )
        return self._async_engine

    @property
    def async_session_maker(self) -> async_sessionmaker[AsyncSession]:
        """
        Public getter for the async session maker.

        Returns
        -------
        async_sessionmaker[AsyncSession]
            The initialized async session maker.

        Raises
        ------
        DatabaseInitializationError
            If the session maker has not been initialized.
        """
        if self._async_session_maker is None:
            raise DatabaseInitializationError(
                'The database is not currently initialized',
            )
        return self._async_session_maker


    def is_open(self) -> bool:
        """
        Check whether the database has an initialized engine.

        Note
        ----
        This currently only checks that an engine has been created and not
        torn down via `aclose()`. It does *not* run a live connectivity check,
        nor does it inspect the underlying pool state.

        Returns
        -------
        bool
            True if an engine is present, False otherwise.
        """
        return self._async_engine is not None

    def configure(self, config: DatabaseConfig) -> None:
        """
        Initialize or reconfigure the async engine and session maker
        from a `DatabaseConfig`.

        Parameters
        ----------
        config : DatabaseConfig
            High-level configuration object describing engine and session
            behavior.

        Warns
        -----
        UserWarning
            If `configure()` is called on an already-open database. In this
            case, the existing engine is left untouched and reconfiguration
            is skipped.
        """
        if self.is_open():
            warnings.warn(
                'configure() cannot be called on an open database; '
                'call aclose() first if you intend to reconfigure.',
                UserWarning,
                stacklevel=2,
            )
            return

        engine_kwargs = config.async_engine_kwargs()
        self._async_engine = create_async_engine(
            self.url,
            **engine_kwargs,
        )

        sessionmaker_kwargs = config.async_sessionmaker_kwargs()
        self._async_session_maker = async_sessionmaker(
            bind=self._async_engine,
            class_=AsyncSession,
            **sessionmaker_kwargs,
        )


    @contextlib.asynccontextmanager
    async def connection(self) -> AsyncIterator[AsyncConnection]:
        """
        Context manager for database connections using the
        `AsyncEngine.connect()`

        Yields
        ------
        AsyncConnection
            An async database connection.

        Example
        --------
        >>> async with db.connection() as conn:
        ...     result = await conn.execute(query)

        Raises
        ------
        DatabaseInitializationError
            If the database has not been initialized.
        """
        async with self.async_engine.connect() as conn:
            yield conn

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession]:
        """
        Context manager for database sessions.

        Yields
        ------
        AsyncSession
            An async database session.

        Example
        --------
        >>> async with db.session() as session:
        ...     session.add(user)
        ...     await session.commit()

        Raises
        ------
        DatabaseInitializationError
            If the database has not been initialized.
        """
        async with self.async_session_maker() as session:
            yield session

    @contextlib.asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """
        Context manager for transactional sessions.

        This wraps `AsyncSession` with `session.begin()`, so that:

        - on normal exit, the transaction is committed
        - on exception, the transaction is rolled back

        Yields
        ------
        AsyncSession
            An async database session with an active transaction.

        Example
        --------
        >>> async with db.transaction() as session:
        ...     session.add(user)
        ...     # Auto-commits on success, rolls back on exception.

        Raises
        ------
        DatabaseInitializationError
            If the database has not been initialized.
        """
        async with self.async_session_maker() as session:
            async with session.begin():
                yield session


    async def aclose(self) -> None:
        """
        Disposesthe underlying async engine and clear the session maker.

        If the database is already closed (no engine present), a warning
        is emitted and the call is a no-op.

        Warns
        -----
        UserWarning
            If `aclose()` is called on an already-closed database.
        """
        if self._async_engine is None:
            warnings.warn(
                'aclose() called on an already-closed database.',
                UserWarning,
                stacklevel=2,
            )
            return

        await self._async_engine.dispose()
        self._async_engine = None
        self._async_session_maker = None

    async def __aenter__(self) -> Self:
        """
        Enter the async context for the database.

        This does *not* automatically configure the engine; you are expected
        to call `configure()` (or use `create(..., engine=...)`) before
        entering the context.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        """
        Exit the async context for the database, closing the engine.

        Behavior
        --------
        - Always attempts to close the engine via `aclose()`.
        - If `aclose()` fails *and* there was no original exception from
          inside the context, the close error is propagated.
        - If `aclose()` fails *and* there *was* an original exception,
          the close error is downgraded to a warning so the original
          exception is not masked.
        - Always returns False so that any exception from the body of the
          context is propagated to the caller.

        Parameters
        ----------
        exc_type : type[BaseException] | None
            Exception type raised inside the context, if any.
        exc : BaseException | None
            Exception instance raised inside the context, if any.
        tb : TracebackType | None
            Traceback associated with the exception, if any.

        Returns
        -------
        bool
            False, indicating that exceptions from the context body should
            never be suppressed.
        """
        try:
            await self.aclose()
        except Exception as close_exc:
            if exc is None:
                raise
            # if there was already an exception, don't mask it with a close error.
            warnings.warn(
                f'Error while closing database during __aexit__: {close_exc!r}',
                RuntimeWarning,
                stacklevel=2,
            )
        return False


    async def ping(self, *, auto_raise: bool = False) -> bool:
        """
        Check connectivity to the database by acquiring a connection
        and executing a simple query.

        Parameters
        ----------
        auto_raise : bool, optional
            If True, any exceptions encountered during the ping are
            re-raised. If False, exceptions are caught and False is
            returned instead, by default False.

        Returns
        -------
        bool
            True if the ping succeeded, False otherwise.

        Raises
        ------
        Exception
            If `auto_raise` is True and an error occurs during the ping.
        """
        try:
            async with self.connection() as conn:
                await conn.execute(sqlalchemy_text('SELECT 1'))
        except Exception:
            if auto_raise:
                raise
            return False
        return True

    @property
    def mapped_models(self) -> dict[str, type[Any]]:
        """
        Get a mapping of registered model class names to their types.

        Returns
        -------
        dict[str, type[DeclarativeBase]]
            A dictionary mapping model class names to their types.
        """
        if self.mapped_base is None:
            return {}

        return {
            cls.__name__: cls
            for cls in self.mapped_base.__subclasses__()
        }

    def register_models(
        self,
        *,
        pattern: str | None = None,
        orm_modules: list[str] | None = None,
    ) -> list[str]:
        """
        Register SQLAlchemy models by importing them
        and maps them to the database's mapped base.

        Parameters
        ----------
        pattern : str | None, optional
            A glob pattern to match module names for importing models,
            by default None

        orm_modules : list[str] | None, optional
            A list of already imported modules containing models,
            by default None

        Returns
        -------
        list[str]
            A list of names of registered model classes.

        Raises
        ------
        ValueError
            If neither `pattern` nor `orm_modules` are provided.
        ImportError
            If importing modules fails.

        Example
        -------
        ```python
        # Register models from modules matching a glob pattern
        # for all packages with a models.py module under 'myapp'.

        db.register_models(pattern='myapp.*.models')
        # Or register models from specific modules.
        db.register_models(orm_modules=[
            'myapp.users.models',
            'myapp.items.models',
        ])
        ```
        """
        return register_sqlalchemy_models(
            base_model=self.mapped_base,
            pattern=pattern,
            orm_modules=orm_modules,
        )

    async def create_tables(self) -> None:
        """
        Create all tables mapped to the database's mapped base.

        This uses the `mapped_base.metadata.create_all()` method
        with the database's async engine.

        Raises
        ------
        DatabaseInitializationError
            If the database has not been initialized.
        """
        if self.mapped_base is None:
            return

        async with self.async_engine.begin() as conn:
            await conn.run_sync(self.mapped_base.metadata.create_all)

    async def drop_tables(self) -> None:
        """
        Drop all tables mapped to the database's mapped base.

        This uses the `mapped_base.metadata.drop_all()` method
        with the database's async engine.

        Raises
        ------
        DatabaseInitializationError
            If the database has not been initialized.
        """
        if self.mapped_base is None:
            return

        async with self.async_engine.begin() as conn:
            await conn.run_sync(self.mapped_base.metadata.drop_all)


