from __future__ import annotations

import contextlib
import dataclasses as dc
import time
import warnings
from typing import TYPE_CHECKING, Any, Literal, Self

from sqlalchemy import URL, Engine, Pool
from sqlalchemy import text as sqlalchemy_text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

if TYPE_CHECKING:
    import logging
    from collections.abc import AsyncGenerator
    from types import TracebackType

    from sqlalchemy.orm import DeclarativeBase


@dc.dataclass(slots=True)
class EngineOptions:
    echo: bool = False
    echo_pool: bool = False
    logging_name: str | None = None
    connect_args: dict[str, Any] | None = None
    execution_options: dict[str, Any] | None = None
    pool_class: type[Pool] | None = None
    pool: Pool | None = None
    pool_pre_ping: bool = False
    pool_size: int | None = None
    pool_recycle: int | None = None
    pool_timeout: float | None = None
    pool_use_lifo: bool = False
    max_overflow: int | None = None

    def to_kwargs(self) -> dict[str, Any]:
        """
        Convert the EngineOptions to a dictionary of keyword arguments
        suitable for passing to create_async_engine.

        Returns
        -------
        dict[str, Any]
            A dictionary of keyword arguments for engine creation.
        """
        dumped = dc.asdict(self)
        return {k: v for k, v in dumped.items() if v is not None}

@dc.dataclass(slots=True)
class SessionOptions:
    """Configuration options for creating an AsyncSessionLocal."""

    expire_on_commit: bool = False
    autoflush: bool = True
    autocommit: bool = False
    class_: type = AsyncSession

    def to_kwargs(self) -> dict[str, Any]:
        return dc.asdict(self)


def async_engine_from_url(
    url: URL | str,
    *,
    options: EngineOptions | None = None,
    **kwargs: Any,
) -> AsyncEngine:
    url = url if isinstance(url, URL) else URL.create(url)
    options = options or EngineOptions()
    engine_kwargs = options.to_kwargs()
    engine_kwargs.update(kwargs)
    return create_async_engine(url, **engine_kwargs)


def make_async_sessionlocal(
    bind: AsyncEngine,
    *,
    options: SessionOptions | None = None,
    **extras: Any,
) -> async_sessionmaker[AsyncSession]:
    options = options or SessionOptions()
    kwargs = options.to_kwargs()
    kwargs.update(extras)
    return async_sessionmaker(bind=bind, **kwargs)

@dc.dataclass(slots=True)
class SqlalchemyConnection:
    async_engine: AsyncEngine
    async_sessionlocal: async_sessionmaker[AsyncSession]

    def get_url(self, **replace: Any) -> URL:
        if replace:
            return self.async_engine.url.set(**replace)
        return self.async_engine.url

    async def aclose(self) -> None:
        await self.async_engine.dispose()

    @property
    def sync_engine(self) -> Engine:
        return self.async_engine.sync_engine

    @contextlib.asynccontextmanager
    async def open_engine(
        self,
        *,
        mode: Literal['begin', 'connect'] | None = None,
    ) -> AsyncGenerator[AsyncConnection]:
        mode = mode or 'connect'
        context_manager = (
            self.async_engine.begin
            if mode == 'begin'
            else self.async_engine.connect
        )
        async with context_manager() as conn:
            yield conn

    @classmethod
    def from_engine(
        cls,
        engine: AsyncEngine,
        *,
        session_options: SessionOptions | None = None,
        session_extras: dict[str, Any] | None = None,
    ) -> Self:
        sessionlocal = make_async_sessionlocal(
            bind=engine,
            options=session_options,
            **(session_extras or {}),
        )
        return cls(
            async_engine=engine,
            async_sessionlocal=sessionlocal,
        )

    @classmethod
    def create(
        cls,
        url: URL | str,
        *,
        engine_options: EngineOptions | None = None,
        engine_extras: dict[str, Any] | None = None,
        session_options: SessionOptions | None = None,
        session_extras: dict[str, Any] | None = None,
    ) -> Self:
        engine = async_engine_from_url(
            url,
            options=engine_options,
            **(engine_extras or {}),
        )
        sessionlocal = make_async_sessionlocal(
            bind=engine,
            options=session_options,
            **(session_extras or {}),
        )
        return cls(
            async_engine=engine,
            async_sessionlocal=sessionlocal,
        )

@dc.dataclass(slots=True)
class AsyncDatabase:
    _connection: SqlalchemyConnection | None = dc.field(default=None, init=False)

    def is_open(self) -> bool:
        return self._connection is not None

    def connect(
        self,
        url: URL | str,
        *,
        engine_options: EngineOptions | None = None,
        engine_extras: dict[str, Any] | None = None,
        session_options: SessionOptions | None = None,
        session_extras: dict[str, Any] | None = None,
    ) -> None:
        if self.is_open():
            warnings.warn(
                'Database connection is already open, '
                'so this call to open_connection() is being ignored.',
                RuntimeWarning,
            )
            return

        self._connection = SqlalchemyConnection.create(
            url,
            engine_options=engine_options,
            engine_extras=engine_extras,
            session_options=session_options,
            session_extras=session_extras,
        )

    async def disconnect(self) -> None:
        if self._connection is not None:
            await self._connection.aclose()
            self._connection = None
        else:
            warnings.warn(
                'Database connection is already closed, '
                'so this call to close_connection() is being ignored.',
                RuntimeWarning,
            )

    @property
    def connection(self) -> SqlalchemyConnection:
        if self._connection is None:
            raise RuntimeError('Database connection is not open')
        return self._connection

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession]:
        async with self.connection.async_sessionlocal() as session:
            yield session

    @contextlib.asynccontextmanager
    async def begin_session(self) -> AsyncGenerator[AsyncSession]:
        async with self.connection.async_sessionlocal() as session:
            async with session.begin():
                yield session

    async def create_metadata(self, base_model: type[DeclarativeBase]) -> None:
        async with self.connection.open_engine(mode='begin') as conn:
            await conn.run_sync(base_model.metadata.create_all)

    async def drop_metadata(self, base_model: type[DeclarativeBase]) -> None:
        async with self.connection.open_engine(mode='begin') as conn:
            await conn.run_sync(base_model.metadata.drop_all)

    async def ping(
        self,
        *,
        auto_raise: bool = False,
        logger: logging.Logger | None = None
    ) -> bool:
        start_time = time.perf_counter()
        try:
            async with self.connection.open_engine() as conn:
                await conn.execute(sqlalchemy_text('SELECT 1'))
        except Exception as exc:
            elapsed = time.perf_counter() - start_time
            if logger:
                logger.exception(
                    f'Database health check failed after {elapsed:.2f} seconds',
                    extra={
                        'exception_type': type(exc).__name__,
                        'exception_message': str(exc),
                        'elapsed_seconds': elapsed,
                    },
                )

            if auto_raise:
                raise

            return False

        elapsed = time.perf_counter() - start_time
        if logger:
            logger.info(
                f'Database health check succeeded in {elapsed:.2f} seconds',
                extra={'elapsed_seconds': elapsed},
            )

        return True

    def __aenter__(self) -> Self:
        if not self.is_open():
            raise RuntimeError('Database connection is not open')
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None
    ) -> bool:
        with contextlib.suppress(Exception):
            await self.disconnect()

        if (exc_type or exc_value or traceback) is not None:
            warnings.warn(
                'Exceptions raised within the async with block '
                'are being suppressed during database disconnect.',
                RuntimeWarning,
            )
            return False

        return True

    def set_atexit_memory_warning(self) -> None:
        import atexit
        import weakref

        weak_self = weakref.ref(self)

        def warn_if_open() -> None:
            self_ref = weak_self()
            if self_ref is not None and self_ref.is_open():
                warnings.warn(
                    'The Database connection was still open at program exit. '
                    'Did you forget to call disconnect()?',
                    stacklevel=2,
                    category=ResourceWarning
                )

        atexit.register(warn_if_open)

    @classmethod
    def from_connection(cls, connection: SqlalchemyConnection) -> Self:
        db = cls()
        db._connection = connection
        return db

def create_database(
    url: URL | str,
    *,
    engine_options: EngineOptions | None = None,
    engine_extras: dict[str, Any] | None = None,
    session_options: SessionOptions | None = None,
    session_extras: dict[str, Any] | None = None,
) -> AsyncDatabase:
    """
    Convenience function to create and configure an `AsyncDatabase`
    from a URL and options.

    Parameters
    ----------
    url : URL | str
        The database URL.

    engine_options : EngineOptions | None, optional
        Configuration options for the SQLAlchemy engine, by default None.

    engine_extras : dict[str, Any] | None, optional
        Additional keyword arguments to pass to the engine creation
        function, by default None.

    session_options : SessionOptions | None, optional
        Configuration options for the SQLAlchemy sessionmaker,
        by default None.

    session_extras : dict[str, Any] | None, optional
        Additional keyword arguments to pass to the sessionmaker creation
        function, by default None.

    Returns
    -------
    AsyncDatabase
        The configured database instance.
    """
    db = AsyncDatabase()
    db.connect(
        url,
        engine_options=engine_options,
        engine_extras=engine_extras,
        session_options=session_options,
        session_extras=session_extras
    )
    return db

def create_database_from_engine(
    engine: AsyncEngine,
    *,
    async_session_maker: async_sessionmaker[AsyncSession] | None = None,
    async_session_options: SessionOptions | None = None,
    async_session_extras: dict[str, Any] | None = None,
) -> AsyncDatabase:
    """
    Convenience function to create and configure an `AsyncDatabase`
    from an existing `AsyncEngine`. Note: The connection will automatically
    be opened since the connection is based on the provided engine; this approach
    generally should be avoided unless you have a specific need to reuse an existing
    engine.

    Parameters
    ----------
    engine : AsyncEngine
        The existing SQLAlchemy async engine.

    async_session_maker : async_sessionmaker[AsyncSession] | None, optional
        An existing sessionmaker to use for creating sessions,
        by default None.

    async_session_options : SessionOptions | None, optional
        Configuration options for the SQLAlchemy sessionmaker if
        `async_session_maker` is not provided, by default None.

    async_session_extras : dict[str, Any] | None, optional
        Additional keyword arguments to pass to the sessionmaker creation
        function if `async_session_maker` is not provided, by default None.

    Returns
    -------
    AsyncDatabase
        The configured database instance.
    """
    if async_session_maker is not None:
        sessionlocal = async_session_maker
    else:
        sessionlocal = make_async_sessionlocal(
            bind=engine,
            options=async_session_options,
            **(async_session_extras or {}),
        )

    connection = SqlalchemyConnection(
        async_engine=engine,
        async_sessionlocal=sessionlocal,
    )

    return AsyncDatabase.from_connection(connection)
