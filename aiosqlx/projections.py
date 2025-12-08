from __future__ import annotations

import dataclasses as dc
from typing import TYPE_CHECKING, Any, TypeVar

from sqlalchemy import Label, Select, select
from sqlalchemy.orm import Load, load_only

if TYPE_CHECKING:
    from sqlalchemy.sql import FromClause
    from sqlalchemy.sql.elements import ColumnElement

T = TypeVar('T')


def _make_column_labels[T](
    columns: dict[str, ColumnElement[Any]],
) -> tuple[Label[Any], ...]:
    """
    Given a mapping of field_name -> ColumnElement, return a tuple of
    labeled columns.
    """
    return tuple(col.label(name) for name, col in columns.items())


@dc.dataclass(slots=True)
class Projection:
    """
    Minimal projection helper for mapping SQLAlchemy rows into dictionaries.
    This class helps define a set of columns to select from a model, and
    provides methods to build SELECT statements and convert results.
    """

    _columns: dict[str, ColumnElement[Any]] = dc.field(init=False, default_factory=dict)
    _labeled_columns: tuple[Label[Any], ...] = dc.field(
        init=False,
        default_factory=tuple,
    )

    def __post_init__(self) -> None:
        self._labeled_columns = _make_column_labels(self._columns)

    @property
    def columns(self) -> dict[str, ColumnElement[Any]]:
        """
        Get the projection's columns mapping.
        """
        return self._columns.copy()

    @property
    def labeled_columns(self) -> tuple[Label[Any], ...]:
        """
        Get the projection's labeled columns.
        """
        return self._labeled_columns

    def update_columns(self, columns: dict[str, ColumnElement[Any]]) -> None:
        """
        Update the projection's columns.

        Parameters
        ----------
        columns : dict[str, ColumnElement]
            Mapping of field_name -> ColumnElement.
        """
        self._columns = columns
        self._labeled_columns = _make_column_labels(columns)

    def select(self, from_clause: FromClause | None = None) -> Select:
        """
        Build a SELECT statement for this projection.

        Parameters
        ----------
        from_clause : FromClause | None, default None
            Optional FROM clause to select from. For ORM models, you can
            instead build your own select and use `with_only_columns`.

        Returns
        -------
        sqlalchemy.sql.Select
            A SELECT statement that selects just this projection's columns.

        Examples
        --------
        >>> stmt = UserSummaryProj.as_select(from_obj=User.__table__).where(...)
        >>> result = await session.execute(stmt)
        """
        stmt = select(*self._labeled_columns)
        if from_clause is not None:
            stmt = stmt.select_from(from_clause)

        return stmt

    def with_only[S: Any](
        self,
        stmt: Select[S],
        *,
        maintain_order: bool = False,
    ) -> Select[S]:
        """
        Applies this projection to an existing SELECT statement.

        This replaces the selected columns with this projection's columns,
        preserving WHERE / ORDER BY / JOIN clauses the caller built.

        Parameters
        ----------
        stmt : Select
            An existing Select object.

        maintain_order : bool, default False
            Whether to maintain the original order of columns in the
            statement. By default, the projection's columns will be in
            the order defined in the projection.

        Returns
        -------
        Select
            A new Select with only this projection's columns.

        Example
        --------
        >>> base = select(User).where(User.is_active == True)
        >>> proj_stmt = UserSummaryProj.with_only(base)
        """
        return stmt.with_only_columns(
            *self._labeled_columns, maintain_order=maintain_order
        )

    def loadonly(self, *, raiseload: bool = False) -> Load:
        """
        Create a Load option that loads only this projection's columns.

        Parameters
        ----------
        raiseload : bool, optional
            Whether to set the columns to raiseload instead of load.
            Default is False.

        Returns
        -------
        Load
            A Load option for use with ORM queries.
        """
        return load_only(*self._labeled_columns, raiseload=raiseload)  # type: ignore


def create_projection(columns: dict[str, ColumnElement[Any]]) -> Projection:
    """
    Create a Projection instance from a mapping of field names to columns.

    Parameters
    ----------
    columns : dict[str, ColumnElement]
        Mapping of field names to SQLAlchemy ColumnElement objects.

    Returns
    -------
    Projection
        A Projection instance with the specified columns.
    """
    proj = Projection()
    proj.update_columns(columns)
    return proj
