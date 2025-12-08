from __future__ import annotations

import time
from typing import TYPE_CHECKING

from sqlalchemy import make_url

from aiosqlx.database import DatabaseInfo, DatabaseInitializationError
from aiosqlx.db._core import SqlalchemyDatabase
from aiosqlx.db._types import DatabaseConfig

if TYPE_CHECKING:
    from sqlalchemy import URL
    from sqlalchemy.orm import DeclarativeBase


def database_from_url(
    *,
    url: URL | str,
    mapped_base: type[DeclarativeBase],
    config: DatabaseConfig | None = None,
) -> SqlalchemyDatabase:
    """
    Convenience function to create and configure a `SqlalchemyDatabase`
    from a URL and `DatabaseConfig`.

    Parameters
    ----------
    url : URL | str
        The database URL.

    mapped_base : type[DeclarativeBase]
        The base class for SQLAlchemy models.

    config : DatabaseConfig | None, optional
        High-level configuration object describing engine and session
        behavior if provided, the database will be created with default
        settings, by default None.

    Returns
    -------
    SqlalchemyDatabase
        The configured database instance.
    """
    if isinstance(url, str):
        url = make_url(url)

    db = SqlalchemyDatabase(url=url, mapped_base=mapped_base)
    config = config or DatabaseConfig()
    db.configure(config)
    return db


async def get_database_info(db: SqlalchemyDatabase) -> DatabaseInfo:
    """
    Retrieve runtime information about the database.

    Parameters
    ----------
    db : SqlalchemyDatabase
        The database instance to inspect.

    Returns
    -------
    DatabaseInfo
        The database information.

    Raises
    ------
    DatabaseInitializationError
        If the database is not open.
    """
    if not db.is_open():
        raise DatabaseInitializationError(
            'Cannot get database info from a closed database',
        )

    start_time = time.perf_counter()
    await db.ping(auto_raise=False)
    latency = time.perf_counter() - start_time
    latency_ms = latency * 1000

    async with db.connection() as conn:
        dialect = conn.dialect

    engine = db.async_engine
    return DatabaseInfo(
        database=db.url.database,
        driver_name=db.url.drivername,
        dialect=dialect.name,
        dialect_description=dialect.dialect_description,
        dialect_dbapi=dialect.dbapi.__name__ if dialect.dbapi else None,
        pool_class=type(engine.pool).__name__,
        echo=engine.echo,
        ping_latency_ms=latency_ms,
    )
