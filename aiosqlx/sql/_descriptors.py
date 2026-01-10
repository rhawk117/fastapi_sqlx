from __future__ import annotations

import abc
import inspect as pyinspect
from dataclasses import dataclass, is_dataclass
from dataclasses import fields as dataclass_fields
from typing import TYPE_CHECKING, Any, Final

from aiosqlx.sql._utils import ExtraOption, sqlalchemy_to_dict, validate_kwargs

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator



class AbstractClassDescriptor[T: Any](abc.ABC):
    def __init__(
        self,
        obj_cls: type[T],
        *,
        factory: Callable[..., T] | None = None
    ) -> None:
        self._obj_cls = obj_cls
        self._attrs: set[str] | None = None
        self._factory: Callable[..., T] | None = factory

    @abc.abstractmethod
    @classmethod
    def get_attributes(cls, obj_cls: type[T]) -> set[str]:
        """Get the set of attribute names defined on the target class.

        Parameters
        ----------
        obj_cls : type[T]
            The target class.

        Returns
        -------
        set[str]
            A set of attribute names.
        """
        ...

    @abc.abstractmethod
    @classmethod
    def describes_class(cls, obj_cls: type[Any]) -> bool:
        """Check if the given class is supported by this descriptor.

        Parameters
        ----------
        obj_cls : type[Any]
            The class to check.

        Returns
        -------
        bool
            True if the class is supported, False otherwise.
        """
        ...

    @property
    def attrs(self) -> set[str]:
        """Get the set of attribute names defined on the
        target class.

        Returns
        -------
        set[str]
            A set of attribute names.
        """
        if self._attrs is None:
            self._attrs = self.get_attributes(self._obj_cls)

        return self._attrs

    def is_attribute(self, name: str) -> bool:
        """Check if the given name is an attribute of the target class.

        Parameters
        ----------
        name : str
            The attribute name to check.

        Returns
        -------
        bool
            True if the name is an attribute of the class, False otherwise.
        """
        return name in self.attrs

    @abc.abstractmethod
    def coerce(self, raw: Any, **options) -> T:
        """Coerce the raw input into an instance of the target class.

        Parameters
        ----------
        raw : Any
            The raw input to coerce.
        **options : Any
            Optional configuration overrides to apply during coercion.

        Returns
        -------
        Any
            An instance of the target class.
        """
        ...

    def convert(self, raw: Any, **options) -> T:
        """Convert the raw input into an instance of the target class.

        This is an alias for the `coerce` method.

        Parameters
        ----------
        raw : Any
            The raw input to convert.
        **options : Any
            Optional configuration overrides to apply during conversion.

        Returns
        -------
        T
            An instance of the target class.
        """
        if not self._factory:
            raise NotImplementedError(
                'Cannot call convert, no factory function defined for class descriptor '
                f'class descriptor of type {self._obj_cls.__name__}'
            )
        return self._factory(raw, **options)

    def is_type(self, type_: type) -> bool:
        return self._obj_cls is type_

    def is_instance(self, obj: Any) -> bool:
        return isinstance(obj, self._obj_cls)


class PyObjectDescriptor[T: Any](AbstractClassDescriptor[T]):
    @classmethod
    def get_attributes(cls, obj_cls: type[T]) -> set[str]:
        if obj_dict := getattr(obj_cls, '__dict__', None):
            return set(obj_dict.keys())

        if obj_slots := getattr(obj_cls, '__slots__', None):
            if isinstance(obj_slots, str):
                return {obj_slots}

            return set(obj_slots)

        return {attr.name for attr in pyinspect.classify_class_attrs(obj_cls)}

    @classmethod
    def describes_class(cls, obj_cls: type[T]) -> bool:
        return pyinspect.isclass(obj_cls)

    def coerce(self, raw: Any, *, extra: ExtraOption) -> T:
        if self.is_instance(raw):
            return raw

        if not (init_kwargs := sqlalchemy_to_dict(raw)):
            raise TypeError(
                f'Cannot coerce object of type {type(raw).__name__} '
                f'to {self._obj_cls.__name__}, could not extract mappings'
            )

        model_attrs = self.attrs
        validate_kwargs(init_kwargs, model_attrs, extra=extra)

        try:
            resolved_object = self._obj_cls(**init_kwargs)
        except TypeError as exc:
            raise TypeError(
                f'Error constructing instance of {self._obj_cls.__name__}: {exc}'
            ) from exc

        return resolved_object


class DataAbstractClassDescriptor[T: Any](PyObjectDescriptor[T]):
    @classmethod
    def get_attributes(cls, obj_cls: type[T]) -> set[str]:
        if not is_dataclass(obj_cls):
            raise TypeError('obj_cls must be a dataclass type')

        return {field.name for field in dataclass_fields(obj_cls)}

    @classmethod
    def describes_class(cls, obj_cls: type[Any]) -> bool:
        return is_dataclass(obj_cls)


type DescriptorTuple = tuple[type[AbstractClassDescriptor], ...]

def default_descriptors() -> DescriptorTuple:
    """Get the default list of class descriptors.

    Returns
    -------
    tuple[AbstractClassDescriptor, ...]
        The default class descriptors.
    """
    return (
        DataAbstractClassDescriptor,
        PyObjectDescriptor
    )

@dataclass(slots=True)
class _DescriptorRegistry:
    _descriptors: DescriptorTuple | None = None

    @property
    def descriptors(self) -> DescriptorTuple:
        """Get the list of descriptors in the registry.

        Returns
        -------
        list[AbstractClassDescriptor[Any]]
            The list of descriptors in load order.
        """
        if self._descriptors is None:
            self._descriptors = default_descriptors()

        return self._descriptors

    @property
    def decriptor_class_names(self) -> list[str]:
        """Get the list of descriptor class names in the registry.

        Returns
        -------
        list[str]
            The list of descriptor class names in load order.
        """
        return [desc_cls.__name__ for desc_cls in self.descriptors]


    def set_order(self, *descriptors: type[AbstractClassDescriptor]) -> None:
        """Set the load order of descriptors in the registry.

        Parameters
        ----------
        *descriptors : AbstractClassDescriptor
            The descriptors to set in the registry, in load order.
        """
        self._descriptors = tuple(descriptors)

    def get_descriptor_for[T: Any](
        self,
        obj_cls: type[T],
        *,
        factory: Callable[..., T] | None = None
    ) -> AbstractClassDescriptor[T]:
        """Get the appropriate descriptor for the given class.

        Parameters
        ----------
        obj_cls : type[T]
            The target class to get a descriptor for.
        factory : Callable[..., T] or None, optional
            An optional factory function to use when coercing instances
            of the target class, by default ``None``.

        Returns
        -------
        AbstractClassDescriptor[T]
            The descriptor for the target class.

        Raises
        ------
        TypeError
            If no suitable descriptor is found for the target class.
        """
        for descriptor_cls in self.descriptors:
            if descriptor_cls.describes_class(obj_cls):
                return descriptor_cls(obj_cls, factory=factory)

        raise TypeError(
            f'No descriptor found for class {obj_cls.__name__} '
            f'known descriptors are: {", ".join(self.decriptor_class_names)}'
        )

    def __iter__(self) -> Iterator[type[AbstractClassDescriptor]]:
        """Iterate over the descriptor classes in the registry.

        Yields
        ------
        Iterator[type[AbstractClassDescriptor]]
            An iterator over the descriptor classes.
        """
        yield from self.descriptors


descriptor_registry: Final[_DescriptorRegistry] = _DescriptorRegistry()
