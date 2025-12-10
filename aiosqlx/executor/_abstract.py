from __future__ import annotations

import abc
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


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
