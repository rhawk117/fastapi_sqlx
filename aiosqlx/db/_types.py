from __future__ import annotations

import dataclasses as dc
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict

import sqlalchemy as sa
from sqlalchemy.orm import Mapper

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.orm.session import JoinTransactionMode


class EngineExtras(TypedDict, total=False):
    """
    Extra keyword arguments for `sqlalchemy.ext.asyncio.create_async_engine()`.

    This mapping is intended as a strongly-typed, namespaced container for
    low-level engine tuning options that you don't want cluttering your
    high-level configuration API. All keys here map directly to keyword
    arguments accepted by `create_async_engine()` / `create_engine()`.

    Keys
    ----
    json_deserializer : Callable[[str], Any]
        Custom JSON deserializer used by dialects that support JSON types.
        Defaults to `json.loads`. Only needed if you want a different JSON
        implementation or custom decoding behavior.

    json_serializer : Callable[[Any], str]
        Custom JSON serializer used when sending JSON parameters to the
        database. Defaults to `json.dumps`.

    isolation_level : str
        Default isolation level for connections produced by the engine
        (e.g. 'READ COMMITTED', 'REPEATABLE READ', 'AUTOCOMMIT').
        Exact values depend on the dialect.

    label_length : int
        Maximum length for auto-generated SQL labels (e.g. column aliases).
        If not set, SQLAlchemy uses an internal default derived from the
        dialect's `max_identifier_length`.

    max_identifier_length : int
        Override the dialect-reported maximum identifier length. Only useful
        for exotic backends or misreported limits.

    module : Any
        Explicit DBAPI module to use instead of the dialect's default.
        Rarely needed unless you are swapping DBAPI implementations manually.

    paramstyle : str
        Override DBAPI parameter style (e.g. 'qmark', 'named', 'pyformat',
        'format', etc.). Typically you should leave this alone unless you
        really know what you're doing.

    plugins : list[str]
        List of `CreateEnginePlugin` names to load. Primarily for frameworks
        or advanced engine customization.

    insertmanyvalues_page_size : int
        Page size for the "insertmanyvalues" bulk INSERT optimization.
        Controls batch size when emitting multi-row INSERTs with RETURNING.

    hide_parameters : bool
        If True, hide actual parameter values in logs and some exceptions.
        Essential in production if you care about not leaking secrets / PII.

    enable_from_linting : bool
        Enable or disable FROM-clause "cartesian product" linting.
        You should usually leave this enabled; disabling it is almost always
        a step backwards.

    use_insertmanyvalues : bool
        Enable or disable the "insertmanyvalues" bulk INSERT optimization.
        You may turn it off if a particular backend has issues with it.

    query_cache_size : int
        Size of the SQL string compilation cache. Set to 0 to disable.
        Tuning this only matters on very large / dynamic query workloads.

    skip_autocommit_rollback : bool
        When True (with supported dialects), prevents `.rollback()` from
        being emitted while the connection is in autocommit mode, avoiding
        an unnecessary round-trip. Mostly relevant for MySQL-style backends.
    """

    json_deserializer: NotRequired[Callable[[str], Any]]
    json_serializer: NotRequired[Callable[[Any], str]]
    isolation_level: NotRequired[str]
    label_length: NotRequired[int]
    max_identifier_length: NotRequired[int]
    module: NotRequired[Any]
    paramstyle: NotRequired[str]
    plugins: NotRequired[list[str]]
    insertmanyvalues_page_size: NotRequired[int]
    hide_parameters: NotRequired[bool]
    enable_from_linting: NotRequired[bool]
    use_insertmanyvalues: NotRequired[bool]
    query_cache_size: NotRequired[int]
    skip_autocommit_rollback: NotRequired[bool]


class SessionMakerExtras(TypedDict, total=False):
    """
    Extra keyword arguments for `sqlalchemy.ext.asyncio.async_sessionmaker()`.

    This mapping contains less commonly used session configuration options
    that you still might want to expose, but not as top-level arguments.

    Keys
    ----
    autobegin : bool
        If True (default), the session implicitly begins a transaction on
        first use. When False, you must explicitly begin a transaction via
        `session.begin()` / `async with session.begin()`.

    close_resets_only : bool
        Controls how `Session.close()` behaves under the hood. When set,
        instructs SQLAlchemy to "reset" internal state instead of fully
        tearing down the session, allowing reuse. This is advanced behavior
        and mirrors `Session.__init__(close_resets_only=...)`.

    twophase : bool
        Enable two-phase (XA-style) commit behavior. Only relevant when you
        are coordinating transactions across multiple databases / resources
        that support two-phase commit.

    info : dict[Any, Any]
        Initial value for `session.info`, a user-defined dictionary stored
        on the session object. Useful for logging, tracing, and attaching
        contextual metadata to a session.

    join_transaction_mode : JoinTransactionMode
        Controls how the session joins into already-existing transactions
        when bound to a `Connection` that has an active transaction.
        Default is usually "conditional_savepoint".
    """

    autobegin: NotRequired[bool]
    close_resets_only: NotRequired[bool]
    twophase: NotRequired[bool]
    info: NotRequired[dict[Any, Any]]
    join_transaction_mode: NotRequired[JoinTransactionMode]


@dc.dataclass(slots=True)
class DatabaseInfo:
    """
    Immutable snapshot of database / engine state for introspection and diagnostics.

    This is intended as a lightweight, read-only summary object describing the
    currently configured engine and its pool metrics. How you populate it is
    up to you (e.g. from `engine.pool`, engine configuration, and a ping).

    Attributes
    ----------
    database : str | None
        Database name or schema currently in use, if known. This may come
        from the URL database component or from a dialect-specific query.

    driver_name : str
        Name of the DBAPI / driver in use (e.g. 'asyncpg', 'psycopg2').

    driver_version : str
        Version string for the DBAPI / driver, if available.

    dialect : str
        Name of the SQLAlchemy dialect (e.g. 'postgresql', 'mysql', 'sqlite').

    pool_class : str
        Fully-qualified or human-readable name of the connection pool class
        in use (e.g. 'QueuePool', 'NullPool').

    pool_size : int
        Configured base size of the pool (number of persistent connections).

    pool_checked_out : int | None
        Number of connections currently checked out from the pool, if it can
        be determined. For some pool implementations this may be unavailable.

    pool_overflow : int | None
        Number of "overflow" connections currently allocated beyond the base
        `pool_size`, if supported by the pool implementation.

    echo : bool
        Whether SQL logging (engine.echo) is currently enabled.

    echo_pool : bool | str
        Whether pool logging is enabled. May be True/False or a string such
        as "debug" for more verbose logging modes.

    pool_pre_ping : bool
        Whether `pool_pre_ping` is enabled, indicating that connections are
        test-pinged before use to detect stale/disconnected connections.

    pool_recycle : int
        Pool recycle timeout in seconds. Connections older than this value
        are closed and replaced on checkout. -1 typically means "no recycle".

    pool_timeout : float | None
        Maximum time in seconds to wait when acquiring a connection from the
        pool before raising a timeout error. May be None if unknown.

    ping_latency_ms : float
        Measured round-trip latency of a simple "ping" query, in milliseconds.
        This is an arbitrary metric you can define in your own health checks.

    is_open : bool
        True if the engine / underlying connection infrastructure is considered
        "open" and usable, False if it has been disposed or is otherwise closed.
    """
    database: str | None
    driver_name: str
    dialect: str
    dialect_description: str
    pool_class: str
    echo: bool
    ping_latency_ms: float
    dialect_dbapi: str | None = None

RestOnReturn = Literal['rollback', 'commit'] | None
"""
Pool reset strategy for `pool_reset_on_return` / `Pool.reset_on_return`.

Values
------
'rollback'
    Call `rollback()` on the DBAPI connection when returned to the pool.
    This is the default and is appropriate for the vast majority of use cases.

'commit'
    Call `commit()` on the DBAPI connection when returned to the pool.
    Can be useful for some databases (e.g. SQL Server) where commit influences
    plan caching, but is more dangerous because it will commit any pending
    work unconditionally.

None
    Do not perform any automatic reset on return. Typically only appropriate
    when using pure autocommit mode or when you implement your own reset
    logic via pool events.
"""


SessionBindKey = type[Any] | Mapper[Any] | str | sa.TableClause
"""
Key type for per-entity bind mappings used with session `binds`.

A `SessionBindKey` can be:

- a mapped ORM class (e.g. `User`)
- a `Mapper` instance
- a string name
- a `TableClause` / table-like object

These are used in maps like `dict[SessionBindKey, SessionBind]` to route
different entities to different engines / connections.
"""

SessionBind = sa.Engine | sa.Connection
"""
Concrete bind target for a `SessionBindKey`.

A `SessionBind` is either:

- an `Engine` (sync engine)
- a `Connection` (sync connection)

For async usage this will generally be the underlying sync engine/connection
wrapped by `AsyncEngine` / `AsyncConnection`.
"""


@dc.dataclass(frozen=True, slots=True)
class DatabaseConfig:
    """
    High-level configuration for async SQLAlchemy engine + session factory.

    This dataclass centralizes all the tunable knobs for:

    - the async engine created via `create_async_engine()`
    - the async session factory created via `async_sessionmaker()`

    It deliberately splits "core" options into explicit fields and pushes
    the long tail of rarely-used options into `engine_extras` and
    `session_extras` so your public API stays readable.

    Engine-related attributes
    -------------------------
    echo : bool, default False
        If True, log all SQL statements issued by the engine.

    echo_pool : bool | str, default False
        If True, log connection pool checkouts/checkins. Some logging setups
        also accept a string such as "debug" for more detailed logging.

    logging_name : str | None, default None
        Optional name suffix for the engine logger
        (`sqlalchemy.engine.Engine[.<name>]`).

    connect_args : dict[str, Any] | None, default None
        Additional keyword arguments passed directly to the DBAPI's
        `connect()` method. This should only contain driver-specific arguments.

    execution_options : dict[str, Any] | None, default None
        Default execution options applied to all connections produced by
        the engine. These correspond to `Connection.execution_options()`.

    pool_class : type[sa.pool.Pool] | None, default None
        Explicit pool implementation to use (e.g. `QueuePool`, `NullPool`).
        If None, SQLAlchemy picks a sensible default for the dialect.

    pool : sa.pool.Pool | None, default None
        Pre-configured pool instance. When provided, this overrides all other
        pool configuration (size, recycle, etc).

    pool_pre_ping : bool, default False
        If True, the engine will emit a cheap "ping" query before handing
        out a connection, discarding and recreating dead connections.

    pool_size : int, default 5
        Base number of connections to maintain in the pool (for `QueuePool`
        and similar).

    pool_recycle : int, default -1
        Number of seconds after which a connection is considered stale and
        will be recycled on next checkout. -1 disables recycling.

    pool_reset_on_return : RestOnReturn, default 'rollback'
        Strategy for resetting connections when returned to the pool. See
        `RestOnReturn` for details.

    pool_timeout : float, default 30.0
        Number of seconds to wait for a connection from the pool before
        raising a timeout error.

    pool_use_lifo : bool, default False
        When True, use LIFO ordering for connection checkout. This allows
        older idle connections to age out and be reclaimed by the server.

    max_overflow : int, default 10
        Maximum number of "overflow" connections beyond `pool_size` that
        may be created during bursts.

    engine_extras : EngineExtras | None, default None
        Additional engine-related keyword arguments that will be forwarded
        directly to `create_async_engine()`.

    Session-related attributes
    --------------------------
    autoflush : bool, default True
        If True, the session automatically flushes pending changes to the
        database before certain operations (e.g. SELECTs).

    expire_on_commit : bool, default True
        If True, ORM instances are expired on commit and will trigger a
        lazy reload on next attribute access. In async code, many people
        prefer `False` to avoid surprise IO after commit.

    autobegin : bool, default True
        If True, each session implicitly starts a transaction on first use.
        If False, you must explicitly begin transactions.

    session_extras : SessionMakerExtras | None, default None
        Additional configuration values forwarded directly to
        `async_sessionmaker()` / `AsyncSession.__init__`.

    Methods
    -------
    async_engine_kwargs() -> dict[str, Any]
        Build the keyword argument dictionary for `create_async_engine()`.

    async_sessionmaker_kwargs() -> dict[str, Any]
        Build the keyword argument dictionary for `async_sessionmaker()`.
    """

    # Engine-level configuration
    echo: bool = False
    echo_pool: bool | str = False
    logging_name: str | None = None
    connect_args: dict[str, Any] | None = None
    execution_options: dict[str, Any] | None = None

    pool_class: type[sa.pool.Pool] | None = None
    pool: sa.pool.Pool | None = None
    pool_pre_ping: bool = False
    pool_size: int = 5
    pool_recycle: int = -1
    pool_reset_on_return: RestOnReturn = 'rollback'
    pool_timeout: float = 30.0
    pool_use_lifo: bool = False
    max_overflow: int = 10

    engine_extras: EngineExtras | None = None

    # async_sessionmaker kwargs
    autoflush: bool = True
    expire_on_commit: bool = True
    autobegin: bool = True
    session_extras: SessionMakerExtras | None = None

    def async_engine_kwargs(self) -> dict[str, Any]:
        """
        Build keyword arguments for `sqlalchemy.ext.asyncio.create_async_engine()`.

        The returned dict is suitable to be splatted into `create_async_engine`
        as `create_async_engine(url, **config.async_engine_kwargs())`.

        Behavior
        --------
        - Includes all core engine-related fields from this config.
        - Merges any `engine_extras` entries, which may override defaults.
        - Filters out keys whose value is `None` so SQLAlchemy can fall back
          to its own defaults where appropriate.
        - Ensures `connect_args` and `execution_options` are always concrete
          dicts (never None).
        """
        base: dict[str, Any] = {
            'echo': self.echo,
            'echo_pool': self.echo_pool,
            'logging_name': self.logging_name,
            'connect_args': self.connect_args or {},
            'execution_options': self.execution_options or {},
            'poolclass': self.pool_class,
            'pool': self.pool,
            'pool_pre_ping': self.pool_pre_ping,
            'pool_size': self.pool_size,
            'pool_recycle': self.pool_recycle,
            'pool_reset_on_return': self.pool_reset_on_return,
            'pool_timeout': self.pool_timeout,
            'pool_use_lifo': self.pool_use_lifo,
            'max_overflow': self.max_overflow,
        }

        if self.engine_extras:
            base.update(self.engine_extras)

        return {key: value for key, value in base.items() if value is not None}

    def async_sessionmaker_kwargs(self) -> dict[str, Any]:
        """
        Build keyword arguments for `sqlalchemy.ext.asyncio.async_sessionmaker()`.

        The returned dict is suitable to be splatted into `async_sessionmaker` as

            async_sessionmaker(bind=engine, **config.async_sessionmaker_kwargs())

        Behavior
        --------
        - Includes common, high-signal flags:
          `autoflush`, `expire_on_commit`, `autobegin`.
        - Merges in any `session_extras`, which may override or extend those.
        - Does not attempt to filter values, since the defaults here are
          already explicit and session-related kwargs are typically all
          meaningful (no "None means use default" semantics for these).
        """
        base: dict[str, Any] = {
            'autoflush': self.autoflush,
            'expire_on_commit': self.expire_on_commit,
            'autobegin': self.autobegin,
        }

        if self.session_extras:
            base.update(self.session_extras)

        return base
