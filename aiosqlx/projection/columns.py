from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING

from sqlalchemy import ColumnElement, Executable

if TYPE_CHECKING:
    from sqlalchemy.sql.selectable import TypedReturnsRows


class ColumnProjection(Mapping[str, ColumnElement]):
    """
    A mapping of column names to SQLAlchemy column expressions.

    This is used to define the columns selected in a query projection.
    """

    __slots__ = ("_columns",)

    def __init__(self, columns: dict[str, ColumnElement]) -> None:
        self._columns = dict(columns)

    def __getitem__(self, key: str) -> ColumnElement:
        return self._columns[key]

    def __setitem__(self, key: str, value: ColumnElement) -> None:
        raise TypeError("ColumnProjection is immutable")

    def __len__(self) -> int:
        return len(self._columns)

    def __iter__(self) -> Iterator[str]:
        return iter(self._columns)

    def copy_with(self, columns: Mapping[str, ColumnElement]) -> ColumnProjection:
        """
        Return a new projection with additional or overridden columns.

        Parameters
        ----------
        columns
            A mapping of column names to SQLAlchemy column expressions to add
            to or override in this projection.
        Returns
        -------
        ColumnProjection
            A new projection with the combined columns.
        """
        new_columns = self._columns.copy()
        new_columns.update(columns)
        return ColumnProjection(new_columns)

    @property
    def labeled_elements(self) -> list[ColumnElement]:
        """
        Get the list of column expressions in this projection.

        Returns
        -------
        list[ColumnElement]
            The list of column expressions.
        """
        return list(col.label(name) for name, col in self._columns.items())

    @property
    def elements(self) -> list[ColumnElement]:
        """
        Get the list of column expressions in this projection.

        Returns
        -------
        list[ColumnElement]
            The list of column expressions.
        """
        return list(self._columns.values())

    def get_elements(self, *, use_labels: bool = True) -> list[ColumnElement]:
        """
        Get the list of column expressions in this projection.

        Parameters
        ----------
        use_labels : bool, optional
            Whether to return labeled columns.

        Returns
        -------
        list[ColumnElement]
            The list of column expressions.
        """
        if use_labels:
            return self.labeled_elements
        return self.elements

    def add_returning(
        self,
        stmnt: Executable,
        *,
        use_labels: bool = True,
    ) -> TypedReturnsRows:
        """
        Add the projection's labeled columns to a statement's `RETURNING` clause.

        Parameters
        ----------
        stmnt :
            An `INSERT`, `UPDATE`, or `DELETE` statement to augment.

        use_labels : bool, optional
            Whether to use labeled columns from the projection.

        Returns
        -------
        Executable
            A new statement with the projection's labeled columns added
            to the `RETURNING` clause.
        """
        if not hasattr(stmnt, 'returning'):
            raise TypeError('Statement does not support RETURNING clause')

        col_seq = self.labeled_elements if use_labels else self.elements
        return stmnt.returning(*col_seq)  # type: ignore[arg-type]


