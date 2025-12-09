# aiosqlx/_sql/binding.py
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    ColumnElement,
    Executable,
    FromClause,
    Label,
    Select,
    delete,
    insert,
    select,
    update,
)
from sqlalchemy.orm import load_only

if TYPE_CHECKING:

    from sqlalchemy.sql.dml import ReturningDelete, ReturningInsert, ReturningUpdate
    from sqlalchemy.sql.elements import BindParameter
    from sqlalchemy.sql.type_api import TypeEngine


def param(
    name: str,
    *,
    type_: TypeEngine[Any] | None = None,
    value: Any | None = None,
    expanding: bool = False,
) -> BindParameter[Any]:
    """
    Convenience wrapper around `sqlalchemy.bindparam`.

    Parameters
    ----------
    name : str
        Name of the bind parameter. Must match the key used when passing
        parameters to `execute()`.

    type_ : TypeEngine | None, default None
        Optional explicit SQLAlchemy type (e.g. `Integer()`, `String()`).
        Usually SQLAlchemy can infer this from the Python value, so this
        is only needed when you want to force a specific type.

    value : Any | None, default None
        Optional default value for this parameter. If provided, the parameter
        may be omitted at execution time and this default will be used.

    expanding : bool | None, default None
        When True, treat this parameter as an "expanding" parameter for
        use in `IN` clauses. SQLAlchemy will expand a sequence value into
        multiple bound parameters (e.g. `IN (:ids_1, :ids_2, :ids_3)`).

    Returns
    -------
    BindParameter
        A SQLAlchemy bind parameter expression.
    """
    return sa_bindparam(
        key=name,
        type_=type_,
        value=value,
        expanding=expanding
    )



class LabeledColumns:
    """
    Represents a collection of columns for frequently used SQL statements.

    """
    __slots__ = ("_columns")

    def __init__(self, columns: dict[str, ColumnElement]) -> None:
        self._columns = columns

    @property
    def elements(self) -> tuple[Label[Any], ...]:
        """
        Get the labels of the columns.
        """
        return tuple(col.label(name) for name, col in self._columns.items())

    @property
    def columns(self) -> dict[str, ColumnElement]:
        """
        Get the labeled columns mapping.
        """
        return self._columns.copy()


    def copy_with(self, columns: dict[str, ColumnElement]) -> LabeledColumns:
        """
        Create a new LabeledColumns instance with updated columns.

        Parameters
        ----------
        columns : dict[str, ColumnElement]
            New mapping of field names to ColumnElement objects.

        Returns
        -------
        LabeledColumns
            A new instance with the updated columns.
        """
        class_columns = self._columns.copy()
        class_columns.update(columns)
        return LabeledColumns(class_columns)

    def update_columns(self, columns: dict[str, ColumnElement]) -> None:
        """
        Update the labeled columns.

        Parameters
        ----------
        columns : dict[str, ColumnElement]
            Mapping of field_name -> ColumnElement.
        """
        self._columns.update(columns)

    def select(self, from_clause: FromClause | None = None) -> Select:
        """
        Build a SELECT statement for the labeled columns.

        Parameters
        ----------
        from_clause : FromClause | None, default None
            Optional FROM clause to select from. For ORM models, you can
            instead build your own select and use `with_only_columns`.

        Returns
        -------
        sqlalchemy.sql.Select
            A SELECT statement that selects just the labeled columns.
        """
        stmnt = select(*self.elements)
        if from_clause is not None:
            stmnt = stmnt.select_from(from_clause)
        return stmnt

    def add_with_only(self, stmt: Select[Any]) -> Select[Any]:
        """
        Modify a SELECT statement to include only the labeled columns.

        Parameters
        ----------
        stmt : Select
            The original SELECT statement.

        Returns
        -------
        Select
            The modified SELECT statement with only the labeled columns.
        """
        return stmt.with_only_columns(*self.elements)

    def as_loadonly(self, *, raiseload: bool = False) -> dict[str, ColumnElement[Any]]:
        """
        Get a copy of the columns mapping.

        Returns
        -------
        dict[str, ColumnElement]
            A copy of the projection's columns mapping.
        """
        return load_only(*self.elements, raiseload=raiseload) # type: ignore

    def returning_update(self, model: type[Any]) -> ReturningUpdate:
        """
        Update the labeled columns based on the model's columns.

        Parameters
        ----------
        model : type[Any]
            The SQLAlchemy model class to extract columns from.
        """
        return update(model).returning(*self.elements)

    def returning_insert(self, model: type[Any]) -> ReturningInsert:
        """
        Create an INSERT statement with RETURNING clause for the labeled columns.

        Parameters
        ----------
        model : type[Any]
            The SQLAlchemy model class to insert into.

        Returns
        -------
        Insert
            An INSERT statement with RETURNING clause for the labeled columns.
        """
        return insert(model).returning(*self.elements)

    def returning_delete(self, model: type[Any]) -> ReturningDelete:
        """
        Create a DELETE statement with RETURNING clause for the labeled columns.

        Parameters
        ----------
        model : type[Any]
            The SQLAlchemy model class to delete from.

        Returns
        -------
        Delete
            A DELETE statement with RETURNING clause for the labeled columns.
        """
        return delete(model).returning(*self.elements)






