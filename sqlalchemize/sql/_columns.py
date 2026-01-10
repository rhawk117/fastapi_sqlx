from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, Self, cast

import sqlalchemy.orm as sa_orm
from sqlalchemy import ColumnElement, Label

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.orm.interfaces import LoaderOption


class ColumnTemplate[C: ColumnElement](abc.ABC):
    """A template for a group of columns or column expressions to define common
    column / expression groupings in a single reusable object. This can be used
    to define sets of columns for loading strategies, creating select statements,
    a returning clause and so on.

    Abstract Methods
    ----------------
    expressions : tuple[C, ...] (property)
        Get the column expressions in this group.

    copy_with(*expressions: C) -> Self
        Create a new column group with extra expressions added.
    """

    @property
    @abc.abstractmethod
    def expressions(self) -> tuple[C, ...]:
        """Get the column expressions in this group.

        Returns
        -------
        tuple[C, ...]
            Tuple of the column expressions in this group.
        """
        ...

    @abc.abstractmethod
    def copy_with(self, *expressions: C) -> Self:
        """Create a new column group with additional expressions.

        The original column group is not modified. New expressions are
        appended to the existing ones.

        Parameters
        ----------
        *expressions : C
            Additional column expressions to add to the group.

        Returns
        -------
        Self
            A new column group with the additional expressions.
        """
        ...

    def __iter__(self) -> Iterator[C]:
        """Iterate over the column expressions."""
        return iter(self.expressions)

    def __len__(self) -> int:
        """Get the number of column expressions."""
        return len(self.expressions)

    def __getitem__(self, index: int) -> C:
        """Get a column expression by index."""
        return self.expressions[index]

    def __contains__(self, expr: Any) -> bool:
        """Check if an expression is in this group."""
        return expr in self.expressions

    def __add__(self, other: Self) -> Self:
        """Combine two column groupings."""
        if not isinstance(other, type(self)):
            return NotImplemented
        return self.copy_with(*other.expressions)

    def __repr__(self) -> str:
        expr_names = ', '.join(str(e) for e in self.expressions)
        return f'{type(self).__name__}({expr_names})'

    @property
    def is_empty(self) -> bool:
        """Check if this grouping has no expressions."""
        return len(self.expressions) == 0

    def loadonly(self, *, raiseload: bool = False) -> LoaderOption:
        """Mark this column group to be loaded only.

        Parameters
        ----------
        raiseload : bool, optional
            If True, raises an error if columns are accessed before loading,
            by default False.

        Returns
        -------
        LoaderOption
            A SQLAlchemy loader option for load-only behavior.
        """
        return sa_orm.load_only(
            *cast('tuple[Any, ...]', self.expressions), raiseload=raiseload
        )

    def joinedload(self, **kwargs: Any) -> LoaderOption:
        """Mark this column group for joined loading.

        Returns
        -------
        LoaderOption
            A SQLAlchemy loader option for joined loading.
        """
        return sa_orm.joinedload(*cast('tuple[Any, ...]', self.expressions), **kwargs)

    def selectinload(self, **kwargs: Any) -> LoaderOption:
        """Mark this column group for selectin loading.

        Returns
        -------
        LoaderOption
            A SQLAlchemy loader option for selectin loading.
        """
        return sa_orm.selectinload(*cast('tuple[Any, ...]', self.expressions), **kwargs)

    def subqueryload(self, **kwargs: Any) -> LoaderOption:
        """Mark this column group for subquery loading.

        Returns
        -------
        LoaderOption
            A SQLAlchemy loader option for subquery loading.
        """
        return sa_orm.subqueryload(*cast('tuple[Any, ...]', self.expressions), **kwargs)


class ColumnGrouping(ColumnTemplate[ColumnElement[Any]]):
    """A concrete grouping of column elements."""

    def __init__(self, *expressions: ColumnElement[Any]) -> None:
        self._expressions: tuple[ColumnElement[Any], ...] = expressions

    @property
    def expressions(self) -> tuple[ColumnElement[Any], ...]:
        """Get the column expressions in this group."""
        return self._expressions

    def copy_with(self, *expressions: ColumnElement[Any]) -> Self:
        """Create a new grouping with additional expressions."""
        new_expressions = self._expressions + expressions
        return type(self)(*new_expressions)

    @classmethod
    def from_model(cls, model: type[Any], *column_names: str) -> Self:
        """Create a column grouping from model columns by name.

        Parameters
        ----------
        model : type[Any]
            The SQLAlchemy model class.
        *column_names : str
            Names of columns to include.

        Returns
        -------
        Self
            A new column grouping with the specified columns.
        """
        columns = tuple(getattr(model, name) for name in column_names)
        return cls(*columns)


class LabeledColumnGrouping(ColumnTemplate[Label[Any]]):
    """A grouping of labeled column expressions."""

    def __init__(self, *labeled_cols: Label[Any]) -> None:
        self._expressions: tuple[Label[Any], ...] = labeled_cols

    def key_pairs(self) -> Iterator[tuple[str, Label[Any]]]:
        """Get an iterator of (name, expression) pairs for the labeled columns.

        Yields
        ------
        tuple[str, Label[Any]]
            A (name, expression) pair for each labeled column.
        """
        for label in self._expressions:
            yield label.name, label

    def as_dict(self) -> dict[str, Label[Any]]:
        """Get a mapping of label names to their expressions.

        Returns
        -------
        dict[str, Label[Any]]
            A dictionary mapping label names to their expressions.
        """
        return dict(self.key_pairs())

    @property
    def expressions(self) -> tuple[Label[Any], ...]:
        """Get the labeled column expressions in this group."""
        return self._expressions

    def copy_with(self, *expressions: Label[Any]) -> Self:
        """Create a new grouping with additional labeled expressions."""
        new_expressions = self._expressions + expressions
        return type(self)(*new_expressions)

    def copy_with_prefix(self, prefix: str, *, sep: str = '_') -> Self:
        """Create a new labeled columns group with a prefix added to each label.

        Parameters
        ----------
        prefix : str
            The prefix to add to each label.
        sep : str, optional
            The separator between the prefix and the original label name,
            by default '_'.

        Returns
        -------
        Self
            A new labeled columns group with prefixed labels.
        """
        new_labels = tuple(
            col.label(f'{prefix}{sep}{col.name}') for col in self._expressions
        )
        return type(self)(*new_labels)
