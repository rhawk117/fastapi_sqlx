import functools
from collections.abc import Callable, Sequence
from typing import Any, NamedTuple

from sqlalchemy import Select
from sqlalchemy.engine import Row
from sqlalchemy.orm import Bundle

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
    """A generated bundler class that uses a specified row processor
    to process rows into instances of type T for type safe automatic
    object creation.
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
                '`__bundle_processor__` is not set and the row processor cannot be '
            )

        context = ProcessorContext(query=query, procs=procs, labels=labels)
        return functools.partial(self.__bundle_processor__, context)


def generate_typed_bundle[T: Any](handler: RowProcessor[T]) -> type[TypedBundle[T]]:
    """Generate a bundler class with the specified row processor.

    Parameters
    ----------
    handler : RowProcessor[T]
        The row processor function to associate with the generated bundler.

    Returns
    -------
    type[TypedBundle[T]]
        A new bundler class with the specified row processor.
    """

    class CustomBundler(TypedBundle[T]):
        __bundle_processor__ = handler

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
    return dict(zip(context.labels, (proc(row) for proc in context.procs)))


def object_bundler[T: Any](object_class: type[T]) -> type[TypedBundle[T]]:
    """Create a bundler class that processes rows into instances of the specified class.

    Parameters
    ----------
    object_class : type[T]
        The class to instantiate for each processed row.

    Returns
    -------
    type[TypedBundle[T]]
        A bundler class type that creates instances of the specified class.
    """

    def _processor(context: ProcessorContext, row: Row[Any]) -> T:
        object_kwargs = _dictionary_processor(context, row)
        return object_class(**object_kwargs)

    return generate_typed_bundle(_processor)


DictBundler = generate_typed_bundle(_dictionary_processor)
ObjectBundler = object_bundler
