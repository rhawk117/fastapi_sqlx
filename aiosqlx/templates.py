from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    ColumnElement,
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
    from collections.abc import Mapping as TypingMapping

    from sqlalchemy.sql.dml import ReturningDelete, ReturningInsert, ReturningUpdate


class ColumnProjection:
    """
    Lightweight helper for defining a named column projection.

    This class ties *logical field names* (e.g. `"id"`, `"email"`) to concrete
    SQLAlchemy column expressions, and can then:

    * Produce labeled columns suitable for `select(...)` and `.returning(...)`
    * Apply the projection to an existing `Select` via `with_only_columns`
    * Build DML statements (INSERT/UPDATE/DELETE) with a `RETURNING` clause
      restricted to the projection
    * Generate an ORM `load_only(...)` option for the same set of attributes

    Typical usage
    -------------
    Define a projection once:

    ```python
    user_projection = ColumnProjection({
        'id': User.id,
        'email': User.email,
        'created_at': User.created_at,
    })
    ```

    Then reuse it consistently across queries:

    * `user_projection.select(User)`
    * `user_projection.returning_insert(User)`
    * `user_projection.add_with_only(select(User).where(...))`
    * `session.execute(select(User).options(user_projection.as_loadonly()))`
    """

    __slots__ = ('_columns',)

    def __init__(self, columns: TypingMapping[str, ColumnElement[Any]]) -> None:
        """
        Initialize a column projection.

        Parameters
        ----------
        columns :
            Mapping of logical field names to SQLAlchemy column expressions.
            The keys are used as labels in generated SELECT/RETURNING clauses.
        """
        self._columns: dict[str, ColumnElement[Any]] = dict(columns)

    @property
    def elements(self) -> tuple[Label[Any], ...]:
        """
        Return the projection as labeled column expressions.

        Each stored column is wrapped with `.label(name)` where `name` is the
        key in the projection mapping.

        This ensures that result rows (or mappings) can be consumed by these
        logical names regardless of the underlying column or expression.

        Returns
        -------
        tuple[Label[Any], ...]
            Tuple of labeled column expressions, in the insertion order of the
            projection mapping.
        """
        return tuple(col.label(name) for name, col in self._columns.items())

    @property
    def columns(self) -> dict[str, ColumnElement[Any]]:
        """
        Return a shallow copy of the underlying column mapping.

        Returns
        -------
        dict[str, ColumnElement[Any]]
            Mapping of logical field name → original `ColumnElement`.
        """
        return self._columns.copy()

    def copy_with(
        self, columns: TypingMapping[str, ColumnElement[Any]]
    ) -> ColumnProjection:
        """
        Return a new projection with additional or overridden columns.

        This is a non-mutating variant of `update_columns`, useful when you
        want to derive a more specific projection from a base one.

        Parameters
        ----------
        columns :
            Mapping of field names to new column expressions. Existing keys
            are overridden, new keys are appended.

        Returns
        -------
        ColumnProjection
            A new projection instance containing the merged mapping.
        """
        merged = self._columns.copy()
        merged.update(columns)
        return ColumnProjection(merged)

    def update_columns(self, columns: TypingMapping[str, ColumnElement[Any]]) -> None:
        """
        Update the projection in place.

        Primarily intended for setup/boot code. At call sites and in
        reusable library code, `copy_with` is usually safer than mutating
        shared instances.

        Parameters
        ----------
        columns :
            Mapping of field name → column expression to add or override.
        """
        self._columns.update(columns)

    def select(self, from_clause: FromClause | None = None) -> Select[Any]:
        """
        Build a `SELECT` statement that returns only the projected columns.

        Parameters
        ----------
        from_clause :
            Optional FROM clause to select from. For ORM models you can pass
            the mapped class or table; alternatively, you can start from an
            existing `Select` and use `add_with_only` instead.

        Returns
        -------
        Select[Any]
            A `SELECT` statement selecting the projection's labeled columns.
        """
        stmt = select(*self.elements)
        if from_clause is not None:
            stmt = stmt.select_from(from_clause)
        return stmt

    def add_with_only(self, stmt: Select[Any]) -> Select[Any]:
        """
        Apply the projection to an existing `SELECT` via `with_only_columns`.

        This preserves the original `FROM`, `WHERE`, `ORDER BY` etc., and
        only replaces the columns in the SELECT list with the projection.

        Parameters
        ----------
        stmt :
            Original `Select` statement to be narrowed to this projection.

        Returns
        -------
        Select[Any]
            A new `Select` with the same core structure but only the
            projection's labeled columns in the SELECT list.
        """
        return stmt.with_only_columns(*self.elements)

    def as_loadonly(self, *, raiseload: bool = False) -> Any:
        """
        Build an ORM `load_only(...)` option for this projection.

        This is useful when you are working with ORM queries and want to
        restrict the loaded attributes to the projection. The keys in the
        projection should correspond to attribute names on the mapped class.

        Parameters
        ----------
        raiseload :
            Passed through to `load_only`. When True, accessing unloaded
            attributes raises instead of silently triggering a lazy load.

        Returns
        -------
        Any
            The ORM loader option returned by `sqlalchemy.orm.load_only`.
        """
        # type: ignore: SQLAlchemy's load_only returns an internal Load type.
        return load_only(*self.elements, raiseload=raiseload)  # type: ignore[arg-type]

    def returning_update(self, model: type[Any]) -> ReturningUpdate:
        """
        Build an `UPDATE` statement with a `RETURNING` projection.

        Parameters
        ----------
        model :
            SQLAlchemy ORM model or table to update.

        Returns
        -------
        ReturningUpdate
            An UPDATE statement whose `RETURNING` clause is restricted
            to this projection's labeled columns.
        """
        return update(model).returning(*self.elements)

    def returning_insert(self, model: type[Any]) -> ReturningInsert:
        """
        Build an `INSERT` statement with a `RETURNING` projection.

        Parameters
        ----------
        model :
            SQLAlchemy ORM model or table to insert into.

        Returns
        -------
        ReturningInsert
            An INSERT statement whose `RETURNING` clause is restricted
            to this projection's labeled columns.
        """
        return insert(model).returning(*self.elements)

    def returning_delete(self, model: type[Any]) -> ReturningDelete:
        """
        Build a `DELETE` statement with a `RETURNING` projection.

        Parameters
        ----------
        model :
            SQLAlchemy ORM model or table to delete from.

        Returns
        -------
        ReturningDelete
            A DELETE statement whose `RETURNING` clause is restricted
            to this projection's labeled columns.
        """
        return delete(model).returning(*self.elements)
