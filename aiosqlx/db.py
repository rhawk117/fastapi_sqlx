from __future__ import annotations

import contextlib
import dataclasses as dc
import time
import warnings
from typing import TYPE_CHECKING, Any, Literal, Self

from sqlalchemy import URL, Engine, Pool
from sqlalchemy import text as sqlalchemy_text
from sqlalchemy.engine import make_url
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
    """
    Configuration options for creating an :class:`AsyncEngine`.

    This dataclass collects common keyword arguments used when calling
    :func:`sqlalchemy.ext.asyncio.create_async_engine`. It can be converted
    into a keyword-argument dictionary via :meth:`to_kwargs`.

    Attributes
    ----------
    echo : bool, default False
        If ``True``, SQLAlchemy will log all SQL statements emitted.
    echo_pool : bool, default False
        If ``True``, log connection pool checkouts/checkins.
    logging_name : str or None, default None
        Name to use for the engine's logger. If ``None``, a default is used.
    connect_args : dict[str, Any] or None, default None
        Extra driver-specific arguments passed to the DBAPI.
    execution_options : dict[str, Any] or None, default None
        Default execution options applied to the engine.
    pool_class : type[Pool] or None, default None
        A custom SQLAlchemy :class:`Pool` class to use.
    pool : Pool or None, default None
        A pre-created :class:`Pool` instance to use. Mutually exclusive with
        ``pool_class``.
    pool_pre_ping : bool, default False
        If ``True``, issue a simple "ping" query on checkout to detect stale
        connections.
    pool_size : int or None, default None
        Size of the connection pool. If ``None``, use SQLAlchemy's default.
    pool_recycle : int or None, default None
        Number of seconds after which connections are recycled. ``None`` means
        no automatic recycling.
    pool_timeout : float or None, default None
        Number of seconds to wait before giving up on getting a connection.
    pool_use_lifo : bool, default False
        If ``True``, use LIFO behavior for pool connections instead of FIFO.
    max_overflow : int or None, default None
        Maximum number of extra connections beyond ``pool_size``. ``None``
        uses SQLAlchemy's default.
    """

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
        Convert the options into keyword arguments for engine creation.

        Returns
        -------
        dict[str, Any]
            A dictionary of keyword arguments suitable for
            :func:`sqlalchemy.ext.asyncio.create_async_engine`. Fields with
            value ``None`` are omitted.
        """
        dumped = dc.asdict(self)
        return {k: v for k, v in dumped.items() if v is not None}


@dc.dataclass(slots=True)
class SessionOptions:
    """
    Configuration options for creating an async sessionmaker.

    This dataclass corresponds to the keyword arguments typically passed to
    :func:`sqlalchemy.ext.asyncio.async_sessionmaker`.

    Attributes
    ----------
    expire_on_commit : bool, default False
        If ``True``, expire all instances on commit. For web apps, ``False``
        is usually more convenient.
    autoflush : bool, default True
        If ``True``, automatically flush pending changes to the database
        before certain operations.
    autocommit : bool, default False
        Deprecated in SQLAlchemy 2.x, should generally remain ``False`` for
        normal ORM usage.
    class_ : type, default AsyncSession
        The session class to be constructed by the sessionmaker.
    """

    expire_on_commit: bool = False
    autoflush: bool = True
    autocommit: bool = False
    class_: type = AsyncSession

    def to_kwargs(self) -> dict[str, Any]:
        """
        Convert the options into keyword arguments for sessionmaker creation.

        Returns
        -------
        dict[str, Any]
            A dictionary of keyword arguments suitable for
            :func:`sqlalchemy.ext.asyncio.async_sessionmaker`.
        """
        return dc.asdict(self)


def async_engine_from_url(
    url: URL | str,
    *,
    options: EngineOptions | None = None,
    **kwargs: Any,
) -> AsyncEngine:
    """
    Create an :class:`AsyncEngine` from a database URL.

    Parameters
    ----------
    url : URL or str
        Database URL. May be a string DSN or a :class:`sqlalchemy.engine.URL`
        instance.
    options : EngineOptions or None, optional
        Engine configuration options. If ``None``, defaults are used.
    **kwargs : Any
        Additional keyword arguments forwarded to
        :func:`sqlalchemy.ext.asyncio.create_async_engine`. These override
        values derived from ``options``.

    Returns
    -------
    AsyncEngine
        A configured async SQLAlchemy engine.

    Notes
    -----
    When a string URL is provided, it is normalized via
    :func:`sqlalchemy.engine.make_url` to ensure consistent behavior and a
    usable :attr:`AsyncEngine.url` property.
    """
    if isinstance(url, str):
        url_obj: URL = make_url(url)
    else:
        url_obj = url

    options = options or EngineOptions()
    engine_kwargs = options.to_kwargs()
    engine_kwargs.update(kwargs)
    return create_async_engine(url_obj, **engine_kwargs)


def make_async_sessionlocal(
    bind: AsyncEngine,
    *,
    options: SessionOptions | None = None,
    **extras: Any,
) -> async_sessionmaker[AsyncSession]:
    """
    Create an async sessionmaker bound to the given engine.

    Parameters
    ----------
    bind : AsyncEngine
        The SQLAlchemy async engine to bind the sessionmaker to.
    options : SessionOptions or None, optional
        Session configuration options. If ``None``, defaults are used.
    **extras : Any
        Additional keyword arguments forwarded to
        :func:`sqlalchemy.ext.asyncio.async_sessionmaker`. These override
        values derived from ``options``.

    Returns
    -------
    async_sessionmaker[AsyncSession]
        A configured async session factory.
    """
    options = options or SessionOptions()
    kwargs = options.to_kwargs()
    kwargs.update(extras)
    return async_sessionmaker(bind=bind, **kwargs)


@dc.dataclass(slots=True)
class SqlalchemyConnection:
    """
    Lightweight wrapper for an async SQLAlchemy engine and sessionmaker.

    Parameters
    ----------
    async_engine : AsyncEngine
        The underlying async engine.
    async_sessionlocal : async_sessionmaker[AsyncSession]
        The sessionmaker used to create :class:`AsyncSession` instances.

    Attributes
    ----------
    async_engine : AsyncEngine
        The underlying async engine.
    async_sessionlocal : async_sessionmaker[AsyncSession]
        The session factory bound to the engine.
    """

    async_engine: AsyncEngine
    async_sessionlocal: async_sessionmaker[AsyncSession]

    def get_url(self, **replace: Any) -> URL:
        """
        Get the database URL, optionally with components replaced.

        Parameters
        ----------
        **replace : Any
            Keyword arguments forwarded to :meth:`URL.set`, such as
            ``database``, ``username``, etc.

        Returns
        -------
        URL
            The original or modified database URL.
        """
        if replace:
            return self.async_engine.url.set(**replace)
        return self.async_engine.url

    async def aclose(self) -> None:
        """
        Dispose the underlying engine and close all connections.

        This will close all pooled connections and free associated resources.
        """
        await self.async_engine.dispose()

    @property
    def sync_engine(self) -> Engine:
        """
        Access the synchronous engine associated with this async engine.

        Returns
        -------
        Engine
            The synchronous SQLAlchemy engine.
        """
        return self.async_engine.sync_engine

    @contextlib.asynccontextmanager
    async def open_engine(
        self,
        *,
        mode: Literal['begin', 'connect'] | None = None,
    ) -> AsyncGenerator[AsyncConnection]:
        """
        Open an async engine connection as a context manager.

        Parameters
        ----------
        mode : {"begin", "connect"} or None, optional
            If ``"connect"``, yields a plain connection (no transaction).
            If ``"begin"``, starts a transaction and yields a connection
            whose context manager will commit/rollback as appropriate.
            If ``None``, defaults to ``"connect"``.

        Yields
        ------
        AsyncConnection
            The active async connection or transactional connection.
        """
        mode = mode or 'connect'
        context_manager = (
            self.async_engine.begin if mode == 'begin' else self.async_engine.connect
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
        """
        Creates a :class:`SqlalchemyConnection` from an existing engine.

        Parameters
        ----------
        engine : AsyncEngine
            The existing async engine.
        session_options : SessionOptions or None, optional
            Options for creating the sessionmaker, by default ``None``.
        session_extras : dict[str, Any] or None, optional
            Additional keyword arguments for the sessionmaker, by default
            ``None``.

        Returns
        -------
        SqlalchemyConnection
            A new connection wrapper that reuses the given engine.
        """
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
        """
        Create a new :class:`SqlalchemyConnection` from a database URL.

        Parameters
        ----------
        url : URL or str
            Database URL.
        engine_options : EngineOptions or None, optional
            Options used to configure the async engine, by default ``None``.
        engine_extras : dict[str, Any] or None, optional
            Additional keyword arguments passed directly to
            :func:`create_async_engine`, by default ``None``.
        session_options : SessionOptions or None, optional
            Options used to configure the sessionmaker, by default ``None``.
        session_extras : dict[str, Any] or None, optional
            Additional keyword arguments passed directly to
            :func:`async_sessionmaker`, by default ``None``.

        Returns
        -------
        SqlalchemyConnection
            The configured connection wrapper.
        """
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
    """
    High-level helper for managing an async SQLAlchemy database connection.

    This class owns a :class:`SqlalchemyConnection` and provides:

    * Centralized engine/session construction via :meth:`connect` or helpers.
    * Context-managed async sessions via :meth:`session` and
      :meth:`begin_session`.
    * Convenience methods for metadata creation and health checks.

    Notes
    -----
    This class does **not** automatically connect unless you created it using,
    which means the ``_connection`` attribute is ``None`` until you call :meth:`connect`
    or one of the creation helpers listed below.

    1. :meth:`connect` or construct it via :func:`create_database`
    2. :func:`create_database_from_engine` before using session or engine helpers.
    """

    _connection: SqlalchemyConnection | None = dc.field(default=None, init=False)

    def is_open(self) -> bool:
        """
        Check whether the database connection wrapper is configured.

        Returns
        -------
        bool
            ``True`` if a :class:`SqlalchemyConnection` is present, ``False``
            otherwise.
        """
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
        """
        Configure the database from a URL.

        This creates an underlying :class:`SqlalchemyConnection`. Engine
        creation is lazy; no actual DB connection is opened until first use.

        Parameters
        ----------
        url : URL or str
            Database URL.
        engine_options : EngineOptions or None, optional
            Engine configuration options, by default ``None``.
        engine_extras : dict[str, Any] or None, optional
            Additional keyword arguments for engine creation, by default
            ``None``.
        session_options : SessionOptions or None, optional
            Session configuration options, by default ``None``.
        session_extras : dict[str, Any] or None, optional
            Additional keyword arguments for sessionmaker creation, by
            default ``None``.

        Warnings
        --------
        RuntimeWarning
            If a connection is already configured, the call is ignored.
        """
        if self.is_open():
            warnings.warn(
                'Database connection is already configured, '
                'so this call to connect() is being ignored.',
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
        """
        Dispose the engine and clear the connection wrapper.

        If no connection has been configured, a warning is emitted and
        nothing else happens.
        """
        if self._connection is not None:
            await self._connection.aclose()
            self._connection = None
        else:
            warnings.warn(
                'Database connection is already closed or was never opened, '
                'so this call to disconnect() is being ignored.',
                RuntimeWarning,
            )

    @property
    def connection(self) -> SqlalchemyConnection:
        """
        Access the underlying :class:`SqlalchemyConnection`.

        Returns
        -------
        SqlalchemyConnection
            The active connection wrapper.

        Raises
        ------
        RuntimeError
            If the database has not been configured via :meth:`connect` or
            one of the creation helpers.
        """
        if self._connection is None:
            raise RuntimeError('Database connection is not open')
        return self._connection

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession]:
        """
        Provide an :class:`AsyncSession` as an async context manager.

        This does **not** automatically begin a transaction. The caller is
        responsible for calling :meth:`AsyncSession.begin` if nee

        Yields
        ------
        AsyncSession
            A new async session bound to the configured engine.
        """
        async with self.connection.async_sessionlocal() as session:
            yield session

    @contextlib.asynccontextmanager
    async def begin_session(self) -> AsyncGenerator[AsyncSession]:
        """
        Provide an :class:`AsyncSession` with a managed transaction.

        A transaction is started via :meth:`AsyncSession.begin`. On exit,
        the transaction is committed if no exception occurred, otherwise
        rolled back.

        Yields
        ------
        AsyncSession
            A new async session with an active transaction.
        """
        async with self.connection.async_sessionlocal() as session:
            async with session.begin():
                yield session

    async def create_metadata(self, base_model: type[DeclarativeBase]) -> None:
        """
        Create all database tables defined on the given declarative base.

        Parameters
        ----------
        base_model : type[DeclarativeBase]
            Declarative ORM base whose ``metadata`` will be created.
        """
        async with self.connection.open_engine(mode='begin') as conn:
            await conn.run_sync(base_model.metadata.create_all)

    async def drop_metadata(self, base_model: type[DeclarativeBase]) -> None:
        """
        Drop all database tables defined on the given declarative base.

        Parameters
        ----------
        base_model : type[DeclarativeBase]
            Declarative ORM base whose ``metadata`` will be dropped.
        """
        async with self.connection.open_engine(mode='begin') as conn:
            await conn.run_sync(base_model.metadata.drop_all)

    async def ping(
        self,
        *,
        auto_raise: bool = False,
        logger: logging.Logger | None = None,
    ) -> bool:
        """
        Perform a simple database health check.

        Executes ``SELECT 1`` on a temporary connection and measures the
        elapsed time.

        Parameters
        ----------
        auto_raise : bool, default False
            If ``True``, re-raise any exception that occurs during the
            health check. If ``False``, the exception is swallowed and
            the method returns ``False``.
        logger : logging.Logger or None, optional
            Optional logger used to record success/failure details, by
            default ``None``.

        Returns
        -------
        bool
            ``True`` if the health check succeeded, ``False`` if it
            failed and ``auto_raise`` is ``False``.

        Raises
        ------
        Exception
            If the health check fails and ``auto_raise`` is ``True``.
        """
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

    async def __aenter__(self) -> Self:
        """
        Enter the async context manager.

        Returns
        -------
        AsyncDatabase
            The current instance.

        Raises
        ------
        RuntimeError
            If the database connection has not been configured.
        """
        if not self.is_open():
            raise RuntimeError('Database connection is not open')
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """
        Exits the async context manager and disconnecting the database
        without raising exceptions.

        Any exception raised inside the ``async with`` block is propagated
        *after* attempting to disconnect.

        Parameters
        ----------
        exc_type : type[BaseException] or None
            Exception type, if one was raised.
        exc_value : BaseException or None
            Exception instance, if one was raised.
        traceback : TracebackType or None
            Traceback, if one was raised.

        Returns
        -------
        bool
            ``False`` so that any exception is not suppressed.

        Warnings
        --------
        RuntimeWarning : stacklevel=2
            If an exception was raised inside the ``async with`` block, a
            warning is emitted indicating that the exception is being
            propagated after disconnecting.
        """
        with contextlib.suppress(Exception):
            await self.disconnect()

        if (exc_type or exc_value or traceback) is not None:
            warnings.warn(
                'Exceptions raised within the async with block are being '
                'propagated after database disconnect().',
                RuntimeWarning,
            )
            return False

        return False

    def set_atexit_memory_warning(self) -> None:
        """
        Register an ``atexit`` hook to warn if the database is still open.

        This is useful in development to catch forgotten :meth:`disconnect`
        calls that might otherwise leave connections dangling.
        """
        import atexit
        import weakref

        weak_self = weakref.ref(self)

        def warn_if_open() -> None:
            self_ref = weak_self()
            if self_ref is not None and self_ref.is_open():
                warnings.warn(
                    'The AsyncDatabase connection was still open at program '
                    'exit. Did you forget to call disconnect()?',
                    stacklevel=2,
                    category=ResourceWarning,
                )

        atexit.register(warn_if_open)

    @classmethod
    def from_connection(cls, connection: SqlalchemyConnection) -> Self:
        """
        Construct an :class:`AsyncDatabase` from an existing connection wrapper.

        Parameters
        ----------
        connection : SqlalchemyConnection
            The connection wrapper to adopt.

        Returns
        -------
        AsyncDatabase
            A database instance using the given connection.
        """
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
    Convenience function to create and configure an :class:`AsyncDatabase`.

    Parameters
    ----------
    url : URL or str
        The database URL.
    engine_options : EngineOptions or None, optional
        Configuration options for the SQLAlchemy engine, by default ``None``.
    engine_extras : dict[str, Any] or None, optional
        Additional keyword arguments passed directly to
        :func:`create_async_engine`, by default ``None``.
    session_options : SessionOptions or None, optional
        Configuration options for the async sessionmaker, by default ``None``.
    session_extras : dict[str, Any] or None, optional
        Additional keyword arguments passed directly to
        :func:`async_sessionmaker`, by default ``None``.

    Returns
    -------
    AsyncDatabase
        The configured database instance.

    Examples
    --------
    Basic construction::

        db = create_database('postgresql+asyncpg://user:pass@localhost/dbname')

    Using custom options::

        engine_opts = EngineOptions(echo=True, pool_pre_ping=True)
        session_opts = SessionOptions(expire_on_commit=False)

        db = create_database(
            'postgresql+asyncpg://user:pass@localhost/dbname',
            engine_options=engine_opts,
            session_options=session_opts,
        )
    """
    db = AsyncDatabase()
    db.connect(
        url,
        engine_options=engine_options,
        engine_extras=engine_extras,
        session_options=session_options,
        session_extras=session_extras,
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
    Create an :class:`AsyncDatabase` from an existing :class:`AsyncEngine`.

    Parameters
    ----------
    engine : AsyncEngine
        The existing SQLAlchemy async engine to reuse.
    async_session_maker : async_sessionmaker[AsyncSession] or None, optional
        An existing async sessionmaker to reuse. If provided, it is used
        directly. If ``None``, a new sessionmaker is created from the
        provided engine, by default ``None``.
    async_session_options : SessionOptions or None, optional
        Configuration options for the sessionmaker when creating a new one,
        by default ``None``.
    async_session_extras : dict[str, Any] or None, optional
        Additional keyword arguments for ``async_sessionmaker`` when
        creating a new one, by default ``None``.

    Returns
    -------
    AsyncDatabase
        The configured database instance that reuses the given engine.

    Notes
    -----
    This helper is most useful in scenarios where the engine is created
    externally (e.g., by a test fixture or a framework integration) and
    you want to wrap it with the higher-level :class:`AsyncDatabase` API.
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
