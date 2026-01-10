from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:

    from sqlalchemy.orm import DeclarativeBase, Mapper


def get_registered_models(mapped_base: type[DeclarativeBase]) -> dict[str, type]:
    """Get a mapping of registered model class names to their types.

    Returns
    -------
    dict[str, type]
        A dictionary mapping model class names to their types.
    """
    result: dict[str, type] = {}
    for mapper in mapped_base.registry.mappers:
        table = getattr(mapper, 'local_table', None)
        if table is not None and hasattr(table, 'name'):
            name = table.name
        else:
            mapped = getattr(mapper, 'mapped_table', None)
            name = getattr(mapped, 'name', mapper.class_.__name__)
        result[name] = mapper.class_

    return result


def get_orm_tablename(
    mapper: Mapper,
    model_class: type,
) -> str:
    """Get the table name for a given ORM mapper and model class.

    Parameters
    ----------
    mapper : Mapper
        The SQLAlchemy ORM mapper.
    model_class : type
        The model class.

    Returns
    -------
    str
        The table name associated with the model class.
    """
    model_table = getattr(mapper, 'local_table', None)

    if model_table and hasattr(model_table, 'name'):
        tablename = model_table.name
    else:
        mapped = getattr(mapper, 'mapped_table', None)
        tablename = getattr(mapped, 'name', model_class.__name__)

    return tablename


class ModelCatalog[B: DeclarativeBase]:
    __slots__ = (
        '_mapped_base',
        'mappers_by_model',
        'models_by_classname',
        'models_by_tablename',
        'tablenames_by_model',
    )

    def __init__(self, mapped_base: type[B], *, auto_compute: bool = True) -> None:
        """Initializes the ModelCatalog.

        Parameters
        ----------
        mapped_base : type[B]
            The SQLAlchemy declarative base class.

        auto_compute : bool, optional
            Whether to automatically compute the model mappings upon
            initialization, by default ``True``.
        """
        self._mapped_base: type[B] = mapped_base
        self.models_by_tablename: dict[str, type[B]] = {}
        self.models_by_classname: dict[str, type[B]] = {}
        self.tablenames_by_model: dict[type[B], str] = {}
        self.mappers_by_model: dict[type[B], Mapper] = {}
        if auto_compute:
            self.initialize()

    def initialize(self) -> None:
        """Resolve and populate the model mappings from the declarative base."""
        if self.models_by_tablename:
            return

        for mapper in self._mapped_base.registry.mappers:
            self.add_mapper(mapper)

    def add_mapper(self, mapper: Mapper) -> None:
        """Adds a mapper to the catalog.

        Parameters
        ----------
        mapper : Mapper
            The SQLAlchemy ORM mapper to add.
        """
        model_class = mapper.class_
        tablename = get_orm_tablename(mapper, model_class)

        self.models_by_tablename[tablename] = model_class
        self.models_by_classname[model_class.__name__] = model_class
        self.tablenames_by_model[model_class] = tablename
        self.mappers_by_model[model_class] = mapper

    def reset(self) -> None:
        """Reset the model mappings, clearing all stored data."""
        self.models_by_tablename.clear()
        self.models_by_classname.clear()
        self.tablenames_by_model.clear()
        self.mappers_by_model.clear()

    @property
    def mapped_base(self) -> type[B]:
        """Get the mapped declarative base class.

        Returns
        -------
        type[B]
            The SQLAlchemy declarative base class.
        """
        return self._mapped_base


    def get_model(
        self, *, tablename: str | None = None, classname: str | None = None
    ) -> type[B] | None:
        """Get the type of a model by table or class name.

        Parameters
        ----------
        tablename : str or None, optional
            The table name to look up, by default ``None``.

        classname : str or None, optional
            The class name to look up, by default ``None``.

        Returns
        -------
        type or None
            The model class if found, otherwise ``None``.
        """
        if not (tablename or classname):
            raise ValueError('Either tablename or classname must be provided')

        if tablename is not None:
            return self.models_by_tablename.get(tablename)

        if classname is not None:
            return self.models_by_classname.get(classname)

        return None

    def get_mapper(self, model_cls: type[B]) -> Mapper | None:
        """Get the mapper for the given model class.

        Parameters
        ----------
        model_cls : type[B]
            The model class to look up.

        Returns
        -------
        Mapper or None
            The mapper if found, otherwise ``None``.
        """
        return self.mappers_by_model.get(model_cls)
