from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, overload

from sqlalchemy.orm import Bundle

if TYPE_CHECKING:
    from sqlalchemy import (
        ColumnElement,
        Insert,
        Update,
    )
    from sqlalchemy.sql.dml import ReturningInsert, ReturningUpdate


class _HasReturning(Protocol):
    """Protocol for SQLAlchemy statements that support RETURNING clauses."""

    def returning(self, *cols: Any) -> Any: ...


class ColumnProjection:
    """
    A reusable projection of named SQLAlchemy column expressions.

    This helper encapsulates a mapping of names to SQLAlchemy ``ColumnElement``
    objects and exposes utilities to:

    - Generate labeled or unlabeled column lists
    - Build ``SELECT`` / ``UPDATE`` / ``INSERT`` statements with a consistent
      ``RETURNING`` projection
    - Build an ORM :class:`~sqlalchemy.orm.Bundle` for the projection

    Parameters
    ----------
    columns : dict[str, ColumnElement]
        Mapping of logical column names to SQLAlchemy column expressions.
        The keys are used as labels when ``labeled=True``.
    """

    def __init__(self, columns: dict[str, ColumnElement]) -> None:
        self._columns = dict(columns)
        self._elements = tuple(columns.values())

    @property
    def elements(self) -> tuple[ColumnElement, ...]:
        """
        Raw column expressions in this projection.

        Returns
        -------
        tuple[ColumnElement, ...]
            Tuple of the underlying column expressions in the order
            they were provided.
        """
        return self._elements

    @property
    def labeled_elements(self) -> list[ColumnElement]:
        """
        Labeled column expressions for this projection.

        Each column is labeled with its corresponding key in the original
        ``columns`` mapping. This is typically what you want for
        ``SELECT`` / ``RETURNING`` projections that will be parsed into DTOs.

        Returns
        -------
        list[ColumnElement]
            List of labeled column expressions.
        """
        return [col.label(name) for name, col in self._columns.items()]

    def copy_with(self, updates: dict[str, ColumnElement]) -> ColumnProjection:
        """Create a new projection with additional or overridden columns.

        The original projection is not modified. Columns with the same key
        in ``updates`` will override the existing ones.

        Parameters
        ----------
        updates : dict[str, ColumnElement]
            Mapping of column names to SQLAlchemy column expressions to add
            or override in this projection.

        Returns
        -------
        ColumnProjection
            A new projection containing the merged column mapping.
        """
        new_columns = self._columns.copy()
        new_columns.update(updates)
        return ColumnProjection(new_columns)

    def get_columns(self, *, labeled: bool = True) -> list[ColumnElement]:
        """
        Get the column expressions for this projection.

        Parameters
        ----------
        labeled : bool, optional
            If ``True``, return labeled columns using the mapping keys as
            labels. If ``False``, return the raw column expressions without
            labels. Default is ``True``.

        Returns
        -------
        list[ColumnElement]
            Column expressions corresponding to this projection.
        """
        if labeled:
            return self.labeled_elements

        return list(self._columns.values())

    @overload
    def add_returning(
        self,
        stmnt: Insert,
        *,
        labeled: bool = True,
    ) -> ReturningInsert: ...

    @overload
    def add_returning(
        self,
        stmnt: Update,
        *,
        labeled: bool = True,
    ) -> ReturningUpdate: ...

    def add_returning(
        self,
        stmnt: Insert | Update | _HasReturning,
        *,
        labeled: bool = True,
    ) -> ReturningInsert | ReturningUpdate | Any:
        return stmnt.returning(*self.get_columns(labeled=labeled))

    def bundle(
        self,
        name: str,
        *,
        use_col_labels: bool = True,
        bundle_label: str | None = None,
    ) -> Bundle:
        """
        Create an ORM :class:`sqlalchemy.orm.Bundle` for this projection.

        Parameters
        ----------
        name : str
            Name of the bundle. This is the attribute under which the bundle
            will appear in ORM result objects.
        use_col_labels : bool, optional
            If ``True``, use labeled columns as the bundle elements.
            If ``False``, use the raw column expressions. Default is ``True``.
        bundle_label : str or None, optional
            Optional label for the bundle itself. If provided, the bundle will
            be labeled with this name in result rows. Default is ``None``.

        Returns
        -------
        Bundle
            A SQLAlchemy :class:`sqlalchemy.orm.Bundle` constructed from this
            projection.
        """
        return Bundle(
            name,
            *self.get_columns(labeled=use_col_labels),
            label=bundle_label,
        )
