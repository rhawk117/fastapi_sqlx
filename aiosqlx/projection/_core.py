from __future__ import annotations

from typing import TYPE_CHECKING, Any, Unpack, final

from sqlalchemy import (
    ColumnElement,
    Executable,
    FromClause,
    Select,
    delete,
    insert,
    select,
    update,
)

from aiosqlx.projection._mapping import ProjectionMapping
from aiosqlx.projection._projectors import (
    Projector,
    PyclassProjector,
    PyclassProjectorOptions,
    PydanticModelValidateOptions,
)

if TYPE_CHECKING:
    from sqlalchemy.sql.selectable import TypedReturnsRows


class Projection[T: Any]:
    """
    A database projection mapping SQLAlchemy columns to a transfer type. This is
    incredibly useful for commonly selected subsets of columns that do not map directly
    to an ORM model and converting an ORM model to a domain model or DTO.

    Attributes
    ----------
    transfer_class : type[T]
        The class type to project to/from, this can be a dataclass, pydantic model,
        or any other class, see the other specializations of this class which have been
        inherited from this one with overloads for conversion functions and ergonomics.
    columns : ProjectionMapping
        The mapping of column names to SQLAlchemy column expressions where the key can
        be used as a label optionally applied to the column expression and the value is
        the SQLAlchemy column expression. You can either supply your own instance or a
        dict[str, ColumnElement] which will be converted to a ProjectionMapping during
        initialization.
    projector : Projector[T]
        The projector function that converts raw database rows or mappings to the
        projection's transfer type.

    Examples
    --------
    ```python
    from dataclasses import dataclass
    import uuid
    from datetime import datetime


    @dataclass
    class UserProfile:
        username: str
        email: str
        id: uuid.UUID
        email_verified_at: datetime | None

        def is_verified(self) -> bool:
            return self.email_verified_at is not None


    user_profile_projection = projection(
        UserProfile,
        columns={
            'username': User.username,
            'email': User.email,
            'id': User.id,
            'email_verified_at': User.email_verified_at,
        },
        projector=DataclassProjector(
            UserProfile, from_attributes=True, exclude_none=False
        ),
    )

    # Create a select statement for the projection
    stmt = user_profile_projection.select().where(User.is_active == True)
    # Execute the statement and convert results to UserProfile instances
    result = await session.execute(stmt)
    user = user_profile_projection.convert(result.mappings().one())
    ```
    """
    def __init__(
        self,
        transfer_class: type[T],
        *,
        columns: ProjectionMapping | dict[str, ColumnElement],
        projector: Projector[T],
    ) -> None:
        self.transfer_class = transfer_class
        if isinstance(columns, dict):
            columns = ProjectionMapping(columns)

        self.columns: ProjectionMapping = columns
        self.projector: Projector[T] = projector

    def select(
        self, from_clause: FromClause | None = None, *, use_labels: bool = True
    ) -> Select:
        """
        Create a SQLAlchemy `select` statement for this projection.

        Parameters
        ----------
        from_clause
            An optional `FromClause` to select from. If not provided, the
            projection columns must be fully qualified.

        use_labels : bool, optional
            Whether to use labeled columns from the projection.

        Returns
        -------
        Executable
            A SQLAlchemy `select` statement selecting the projection columns.
        """

        elements = self.columns.get_elements(use_labels=use_labels)
        stmnt = select(*elements)
        if from_clause is not None:
            stmnt = stmnt.select_from(from_clause)
        return stmnt

    def returning(
        self,
        stmnt: Executable,
        *,
        use_labels: bool = True,
    ) -> TypedReturnsRows:
        """
        Add the projection's columns to a statement's `RETURNING` clause.

        Parameters
        ----------
        stmnt
            The statement to add the `RETURNING` clause to.

        use_labels : bool, optional
            Whether to return labeled columns.

        Returns
        -------
        TypedReturnsRows
            The statement with the added `RETURNING` clause.
        """
        elements = self.columns.get_elements(use_labels=use_labels)
        return stmnt.returning(*elements)  # type: ignore[arg-type]

    def insert_returning(
        self,
        model: type[Any],
        *,
        use_labels: bool = True,
    ) -> TypedReturnsRows:
        """
        Create an `INSERT ... RETURNING` statement for this projection.

        Parameters
        ----------
        model : type[Any]
            The model (table) to insert into.

        use_labels : bool, optional
            Whether to return labeled columns.

        Returns
        -------
        TypedReturnsRows
            An `INSERT` statement with a `RETURNING` clause for the projection.
        """
        stmnt = insert(model)
        return self.returning(stmnt, use_labels=use_labels)

    def update_returning(
        self,
        model: type[Any],
        *,
        use_labels: bool = True,
    ) -> TypedReturnsRows:
        """
        Create an `UPDATE ... RETURNING` statement for this projection.

        Parameters
        ----------
        model : type[Any]
            The model (table) to update.

        use_labels : bool, optional
            Whether to return labeled columns.

        Returns
        -------
        TypedReturnsRows
            An `UPDATE` statement with a `RETURNING` clause for the projection.
        """
        stmnt = update(model)
        return self.returning(stmnt, use_labels=use_labels)

    def delete_returning(
        self,
        model: type[Any],
        *,
        use_labels: bool = True,
    ) -> TypedReturnsRows:
        """
        Create a `DELETE ... RETURNING` statement for this projection.

        Parameters
        ----------
        model : type[Any]
            The model (table) to delete from.

        use_labels : bool, optional
            Whether to return labeled columns.

        Returns
        -------
        TypedReturnsRows
            A `DELETE` statement with a `RETURNING` clause for the projection.
        """
        stmnt = delete(model)
        return self.returning(stmnt, use_labels=use_labels)

    def add_with_only(
        self,
        stmnt: Select,
        *,
        use_labels: bool = True,
    ) -> Select:
        """
        Add a `WITH ONLY` clause to a `SELECT` statement for this projection.

        Parameters
        ----------
        stmnt : Select
            The `SELECT` statement to augment.
        use_labels : bool, optional
            Whether to use labeled columns from the projection.
        Returns
        -------
        Select
            The augmented `SELECT` statement with a `WITH ONLY` clause.
        """
        columns = self.columns.get_elements(use_labels=use_labels)
        return stmnt.with_only_columns(*columns)

    def convert(self, obj: Any, **options: Any) -> T:
        """
        Convert a raw database row or mapping to the projection's transfer type.

        Parameters
        ----------
        obj : Any
            The raw database row or mapping to convert.
        options : Any
            Additional options to pass to the projector.

        Returns
        -------
        T
            An instance of the projection's transfer type.
        """
        return self.projector(obj, **options)

    def convert_all(self, objs: list[Any], **options: Any) -> list[T]:
        """
        Convert a list of raw database rows or mappings to the projection's transfer
        type.

        Parameters
        ----------
        objs : list[Any]
            The list of raw database rows or mappings to convert.
        options : Any
            Additional options to pass to the projector.

        Returns
        -------
        list[T]
            A list of instances of the projection's transfer type.
        """
        return [self.convert(obj, **options) for obj in objs]

    def get_fields(self) -> list[str]:
        """
        Get the list of field names in this projection.

        Returns
        -------
        list[str]
            The list of field names.
        """
        return list(self.columns.keys())


class PyclassProjection[T: Any](Projection[T]):
    def __init__(
        self,
        transfer_class: type[T],
        *,
        columns: ProjectionMapping | dict[str, ColumnElement],
        **options: Unpack[PyclassProjectorOptions]
    ) -> None:
        projector = PyclassProjector(transfer_class, **options)
        super().__init__(
            transfer_class,
            columns=columns,
            projector=projector,
        )

    def convert(self, obj: Any, **options: Unpack[PyclassProjectorOptions]) -> T:
        """
        Convert a raw database row or mapping to the projection's transfer type.

        Parameters
        ----------
        obj : Any
            The raw database row or mapping to convert.
        options : Any
            Additional options to pass to the projector.

        Returns
        -------
        T
            An instance of the projection's transfer type.
        """
        return self.projector(obj, **options)

    def convert_all(
        self,
        objs: list[Any],
        **options: Unpack[PyclassProjectorOptions]
    ) -> list[T]:
        """
        Convert a list of raw database rows or mappings to the projection's transfer
        type.

        Parameters
        ----------
        objs : list[Any]
            The list of raw database rows or mappings to convert.
        options : Any
            Additional options to pass to the projector.

        Returns
        -------
        list[T]
            A list of instances of the projection's transfer type.
        """
        return [self.convert(obj, **options) for obj in objs]

@final
class DataclassProjection[T: Any](PyclassProjection[T]): ...


@final
class PydanticProjection[T: Any](Projection[T]):
    def __init__(
        self,
        transfer_class: type[T],
        *,
        columns: ProjectionMapping | dict[str, ColumnElement],
        **options: Unpack[PydanticModelValidateOptions]
    ) -> None:
        projector = PyclassProjector(transfer_class, **options)
        super().__init__(
            transfer_class,
            columns=columns,
            projector=projector
        )

    def convert(self, obj: Any, **options: Unpack[PydanticModelValidateOptions]) -> T:
        """
        Convert a raw database row or mapping to the projection's transfer type.
        Parameters
        ----------
        obj : Any
            The raw database row or mapping to convert.
        options : Any
            Additional options to pass to the projector.
        Returns
        -------
        T
            An instance of the projection's transfer type.
        """
        return self.projector(obj, **options)

    def convert_all(
        self,
        objs: list[Any],
        **options: Unpack[PydanticModelValidateOptions]
    ) -> list[T]:
        """
        Convert a list of raw database rows or mappings to the projection's transfer
        type.

        Parameters
        ----------
        objs : list[Any]
            The list of raw database rows or mappings to convert.
        options : Any
            Additional options to pass to the projector.

        Returns
        -------
        list[T]
            A list of instances of the projection's transfer type.
        """
        return [self.convert(obj, **options) for obj in objs]


def projection[T](
    obj: type[T],
    *,
    columns: ProjectionMapping | dict[str, ColumnElement],
    projector: Projector[T] | None = None,
) -> Projection[T]:
    """
    Create a projection for the given transfer class and columns

    Parameters
    ----------
    obj : type[T]
        The transfer class type.

    columns : ProjectionMapping | dict[str, ColumnElement]
        The mapping of column names to SQLAlchemy column expressions.

    projector : Projector[T] | None, optional
        An optional custom projector. If not provided, a default projector
        will be created based on the transfer class type.

    Returns
    -------
    Projection[T]
        The created projection.
    """
    if projector is None:
        projector = PyclassProjector(
            obj,
            from_attributes=True,
            exclude_none=False
        )

    return Projection(
        obj,
        columns=columns,
        projector=projector,
    )

def class_projection[T](
    transfer_class: type[T],
    *,
    columns: ProjectionMapping | dict[str, ColumnElement],
    **options: Unpack[PyclassProjectorOptions]
) -> PyclassProjection[T]:
    """
    Create a PyclassProjection for the given transfer class and columns

    Parameters
    ----------
    transfer_class : type[T]
        The transfer class type.

    columns : ProjectionMapping | dict[str, ColumnElement]
        The mapping of column names to SQLAlchemy column expressions.

    options : Unpack[PyclassProjectorOptions]
        Additional options to pass to the PyclassProjector.

    Returns
    -------
    PyclassProjection[T]
        The created PyclassProjection.
    """
    return PyclassProjection(
        transfer_class,
        columns=columns,
        **options,
    )

def dataclass_projection[T](
    transfer_class: type[T],
    *,
    columns: ProjectionMapping | dict[str, ColumnElement],
    **options: Unpack[PyclassProjectorOptions]
) -> DataclassProjection[T]:
    """
    Create a DataclassProjection for the given transfer class and columns

    Parameters
    ----------
    transfer_class : type[T]
        The transfer class type.

    columns : ProjectionMapping | dict[str, ColumnElement]
        The mapping of column names to SQLAlchemy column expressions.

    options : Unpack[PyclassProjectorOptions]
        Additional options to pass to the PyclassProjector.

    Returns
    -------
    DataclassProjection[T]
        The created DataclassProjection.
    """
    return DataclassProjection(
        transfer_class,
        columns=columns,
        **options,
    )

def pydantic_projection[T](
    transfer_class: type[T],
    *,
    columns: ProjectionMapping | dict[str, ColumnElement],
    **options: Unpack[PydanticModelValidateOptions]
) -> PydanticProjection[T]:
    """
    Create a PydanticProjection for the given transfer class and columns

    Parameters
    ----------
    transfer_class : type[T]
        The transfer class type.

    columns : ProjectionMapping | dict[str, ColumnElement]
        The mapping of column names to SQLAlchemy column expressions.

    options : Unpack[PydanticModelValidateOptions]
        Additional options to pass to the PyclassProjector.

    Returns
    -------
    PydanticProjection[T]
        The created PydanticProjection.
    """
    return PydanticProjection(
        transfer_class,
        columns=columns,
        **options
    )
