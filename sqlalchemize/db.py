from __future__ import annotations

import contextlib
import dataclasses as dc
import warnings
from typing import TYPE_CHECKING, Any, Literal, Self

from sqlalchemy import URL, Connection, Engine, Pool
from sqlalchemy import text as sqlalchemy_text
from sqlalchemy.engine import make_url as sa_make_url
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

from sqlalchemize.orm import ModelCatalog

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator
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


type EngineTypes = AsyncEngine | Engine
type SessionmakerTypes = async_sessionmaker[AsyncSession] | sessionmaker[Session]


class _SQLAlchemyDatabase[E: EngineTypes, S: Any, B: DeclarativeBase]:
    models: ModelCatalog[B]
    _engine: E
    _sessionmaker: S
    _url: URL

    def __init__(
        self,
        *,
        engine: E,
        sessionmaker: S,
        mapped_base: type[B],
        url: URL | str | None = None,
    ) -> None:
        if isinstance(url, str):
            url = sa_make_url(url)
        elif url is None:
            url = engine.url  # type: ignore[attr-defined]

        self.models: ModelCatalog[B] = ModelCatalog(mapped_base)
        self._engine: E = engine
        self._sessionmaker: S = sessionmaker
        self._url: URL = url  # type: ignore[assignment]

    def get_url(self, **replace: Any) -> URL:
        """Get the database URL, optionally with keyword components
        to replace in the returned URL.

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
            return self._url.set(**replace)

        return self._url



class AsyncSQLDatabase[B: DeclarativeBase](
    _SQLAlchemyDatabase[AsyncEngine, async_sessionmaker[AsyncSession], B]
):
    async def aclose(self) -> None:
        """Dispose the underlying engine and close all connections.

        This will close all pooled connections and free associated resources.
        """
        await self._engine.dispose()

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession]:
        """Provide an :class:`AsyncSession` as an async context manager.

        This does **not** automatically begin a transaction. The caller is
        responsible for calling :meth:`AsyncSession.begin` if need be.

        Yields
        ------
        AsyncSession
            A new async session bound to the configured engine.
        """
        async with self._sessionmaker() as session:
            yield session

    @contextlib.asynccontextmanager
    async def engine_connection(
        self, mode: Literal['begin', 'connect'] | None = None
    ) -> AsyncGenerator[AsyncConnection]:
        """Open an async engine connection as a context manager.

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
            self._engine.begin if mode == 'begin' else self._engine.connect
        )
        async with context_manager() as conn:
            yield conn

    async def ping(self) -> bool:
        """Perform a simple database health check.

        Executes ``SELECT 1`` on a temporary connection.

        Returns
        -------
        bool
            ``True`` if the health check succeeded, ``False`` otherwise.
        """
        try:
            async with self.engine_connection() as conn:
                await conn.execute(sqlalchemy_text('SELECT 1'))
        except Exception:
            return False

        return True

    async def __aenter__(self) -> Self:
        """Enter the async context manager.

        Returns
        -------
        AsyncSQLAlchemyDatabase[B]
            The current instance.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Exits the async context manager and disposing the engine.

        Any exception raised inside the ``async with`` block is propagated
        *after* attempting to dispose the engine.

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
            propagated after disposing.
        """
        with contextlib.suppress(Exception):
            await self.aclose()

        if (exc_type or exc_value or traceback) is not None:
            warnings.warn(
                'Exceptions raised within the async with block are being '
                'propagated after database aclose().',
                RuntimeWarning,
            )

        return False



class SQLDatabase[B: DeclarativeBase](
    _SQLAlchemyDatabase[Engine, sessionmaker[Session], B]
):
    def close(self) -> None:
        """Dispose the underlying engine and close all connections.

        This will close all pooled connections and free associated resources.
        """
        self._engine.dispose()

    @contextlib.contextmanager
    def session(self) -> Generator[Session]:
        """Provide a :class:`Session` as a context manager.

        This does **not** automatically begin a transaction. The caller is
        responsible for calling :meth:`Session.begin` if need be.

        Yields
        ------
        Session
            A new session bound to the configured engine.
        """
        with self._sessionmaker() as session:
            yield session

    @contextlib.contextmanager
    def engine_connection(
        self,
        mode: Literal['begin', 'connect'] | None = None,
    ) -> Generator[Connection]:
        """Open an engine connection as a context manager.

        Parameters
        ----------
        mode : {"begin", "connect"} or None, optional
            If ``"connect"``, yields a plain connection (no transaction).
            If ``"begin"``, starts a transaction and yields a connection
            whose context manager will commit/rollback as appropriate.
            If ``None``, defaults to ``"connect"``.

        Yields
        ------
        Connection
            The active connection or transactional connection.
        """
        mode = mode or 'connect'
        context_manager = (
            self._engine.begin if mode == 'begin' else self._engine.connect
        )
        with context_manager() as conn:
            yield conn

    def ping(self) -> bool:
        """Perform a simple database health check.

        Executes ``SELECT 1`` on a temporary connection.

        Returns
        -------
        bool
            ``True`` if the health check succeeded, ``False`` otherwise.
        """
        try:
            with self.engine_connection() as conn:
                conn.execute(sqlalchemy_text('SELECT 1'))
        except Exception:
            return False

        return True

    def __enter__(self) -> Self:
        """Enter the context manager.

        Returns
        -------
        SQLAlchemyDatabase[B]
            The current instance.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        """Exits the context manager and disposing the engine.

        Any exception raised inside the ``with`` block is propagated *after*
        attempting to dispose the engine.

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
            If an exception was raised inside the ``with`` block, a warning
            is emitted indicating that the exception is being propagated after
            disposing.
        """
        with contextlib.suppress(Exception):
            self.close()

        if (exc_type or exc_value or traceback) is not None:
            warnings.warn(
                'Exceptions raised within the with block are being '
                'propagated after database close().',
                RuntimeWarning,
            )

        return False






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
