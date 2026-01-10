from __future__ import annotations

import functools
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, NamedTuple

from sqlalchemy.engine import Row
from sqlalchemy.orm import Bundle

if TYPE_CHECKING:
    from sqlalchemy import Select

ProcFunc = Callable[[Row[Any]], Any]

type RowProcessor[T] = Callable[[ProcessorContext, Row[Any]], T]


class ProcessorContext(NamedTuple):
    """The context for processing rows in a bundler in the `create_row_processor` method

    Attributes
    ----------
    query : Select[Any]
        The SQLAlchemy Select query the bundle is in
    procs : Sequence[ProcFunc]
        The sequence of processor functions for each column in the bundle
    labels : Sequence[str]
        The sequence of labels for each column in the bundle
    """

    query: Select[Any]
    procs: Sequence[ProcFunc]
    labels: Sequence[str]


class TypedBundle[T: Any](Bundle[T]):
    """A generated bundler class that uses the specified row processoruser_out
    to process rows into instances of type T for type safe automatic
    object creation in queries with provides an ergonomic way to automatically
    map database rows to Python objects.

    The `RowProcessor` function specified in the `__bundle_processor__` attribute
    and is used to override the `sqlalchemy.orm.Bundle.create_row_processor` method
    allowing this to be generated functionally without remembering the the method
    signature.

    Example usage:
    ```python
    from sqlalchemize import DataclassBundler
    import dataclasses as dc


    @dc.dataclass
    class UserOut:
        id: int
        name: str
        email: str


    # Create a bundle class for the User dataclass
    UserBundle = DataclassBundler(UserOut)
    user_out = UserBundle('user_out', User.id, User.name, User.email)

    # Use the UserBundle in a SQLAlchemy query
    from sqlalchemy import select

    statement = select(user_out).where(User.active == True)
    results = await session.scalars(statement)
    # ^ results will be of type ScalarResult[UserOut]

    for user in results.all():
        print(user.name, user.email)
    ```
    """
    __bundle_processor__: RowProcessor[T]

    def create_row_processor(
        self,
        query: Select[Any],
        procs: Sequence[Callable[[Row[Any]], Any]],
        labels: Sequence[str],
    ) -> Callable[[Row[Any]], T]:
        if not self.__bundle_processor__:
            raise RuntimeError(
                '`__bundle_processor__` is None and the row processor cannot be created'
            )

        context = ProcessorContext(query=query, procs=procs, labels=labels)
        return functools.partial(self.__bundle_processor__, context)


def generate_typed_bundle[T: Any](
    handler: RowProcessor[T], *, name: str | None = None
) -> type[TypedBundle[T]]:
    """Generate a bundler class with the specified row processor.

    Parameters
    ----------
    handler : RowProcessor[T]
        The row processor function to associate with the generated bundler.
    name : str | None, optional
        Custom name for the generated class. If None, uses the handler's name.

    Returns
    -------
    type[TypedBundle[T]]
        A new bundler class with the specified row processor.
    """
    class_name = name or f'{handler.__name__.title().replace("_", "")}Bundle'

    class CustomBundler(TypedBundle[T]):
        __bundle_processor__ = handler

    CustomBundler.__name__ = class_name
    CustomBundler.__qualname__ = class_name

    return CustomBundler


def _dictionary_processor(context: ProcessorContext, row: Row[Any]) -> dict[str, Any]:
    """Process a row into a dictionary using the provided context.

    Parameters
    ----------
    context : ProcessorContext
        The context containing query, processors, and labels.
    row : Row[Any]
        The database row to process.

    Returns
    -------
    dict[str, Any]
        A dictionary mapping labels to processed row values.
    """
    return dict(zip(context.labels, (proc(row) for proc in context.procs), strict=True))


DictionaryBundle = generate_typed_bundle(_dictionary_processor, name='DictionaryBundle')


def ObjectBundler[T: Any](  # noqa: N802
    object_class: type[T], *, strict: bool = True
) -> type[TypedBundle[T]]:
    """Create a bundler class that processes rows into instances of the specified class.

    Parameters
    ----------
    object_class : type[T]
        The class to instantiate for each processed row.
    strict : bool, optional
        If True, raises TypeError if extra keys exist that aren't class attributes.
        Default is True.

    Returns
    -------
    type[TypedBundle[T]]
        A bundler class type that creates instances of the specified class.
    """

    def _processor(context: ProcessorContext, row: Row[Any]) -> T:
        object_kwargs = _dictionary_processor(context, row)

        if strict:
            valid_attrs = {k for k in dir(object_class) if not k.startswith('_')}
            extra_keys = set(object_kwargs.keys()) - valid_attrs
            if extra_keys:
                raise TypeError(
                    f'Got unexpected keyword arguments for {object_class.__name__}: '
                    f'{", ".join(sorted(extra_keys))}'
                )

        return object_class(**object_kwargs)

    return generate_typed_bundle(_processor, name=f'{object_class.__name__}Bundler')


def DataclassBundler[T: Any](dataclass_type: type[T]) -> type[TypedBundle[T]]:  # noqa: N802
    """Create a bundler for dataclass types with field validation.

    Parameters
    ----------
    dataclass_type : type[T]
        The dataclass to instantiate for each processed row.

    Returns
    -------
    type[TypedBundle[T]]
        A bundler class type that creates dataclass instances.
    """
    from dataclasses import fields, is_dataclass

    if not is_dataclass(dataclass_type):
        raise TypeError(f'{dataclass_type.__name__} is not a dataclass')

    field_names = {f.name for f in fields(dataclass_type)}

    def _processor(context: ProcessorContext, row: Row[Any]) -> T:
        object_kwargs = _dictionary_processor(context, row)

        filtered_kwargs = {k: v for k, v in object_kwargs.items() if k in field_names}

        return dataclass_type(**filtered_kwargs)  # type: ignore

    return generate_typed_bundle(_processor, name=f'{dataclass_type.__name__}Bundler')
