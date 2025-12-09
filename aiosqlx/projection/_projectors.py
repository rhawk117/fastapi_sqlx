from __future__ import annotations

import warnings
from dataclasses import fields as dataclass_fields
from dataclasses import is_dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NotRequired,
    Protocol,
    Self,
    TypedDict,
    Unpack,
)

if TYPE_CHECKING:

    try:
        from pydantic import BaseModel
    except ImportError:

        class BaseModel(Protocol):
            @classmethod
            def model_validate(
                cls,
                obj: Any,
                *,
                strict: bool | None = None,
                extra: Literal['allow', 'ignore', 'forbid'] | None = None,
                from_attributes: bool | None = None,
                context: Any | None = None,
                by_alias: bool | None = None,
                by_name: bool | None = None,
            ) -> Self: ...

            @classmethod
            def parse_obj(cls, obj: Any) -> Self: ...


class PyclassProjectorOptions(TypedDict, total=False):
    from_attributes: NotRequired[bool]
    exclude_none: NotRequired[bool]
    forbid_extra: NotRequired[bool]


class PydanticModelValidateOptions(TypedDict, total=False):
    strict: NotRequired[bool]
    extra: NotRequired[Literal['allow', 'ignore', 'forbid']]
    from_attributes: NotRequired[bool]
    context: Any
    by_alias: NotRequired[bool]
    by_name: NotRequired[bool]


class Projector[T](Protocol):
    def __call__(self, obj: Any, **overrides: Any) -> T: ...


class PyclassProjector[T](Projector[T]):
    """
    Generic projector for plain Python classes (including dataclasses).

    It constructs `projected_type` instances using keyword arguments, with
    configurable behavior on where to read data from and how to treat extra
    or null values.

    Options
    -------
    from_attributes :
        * True  -> read attributes from `obj` via `getattr`
        * False -> treat `obj` as a mapping (e.g. dict / RowMapping)

    exclude_none :
        If True, drop fields whose value is `None` before calling the
        constructor. This allows constructor defaults to apply.

    forbid_extra :
        If True and `obj` is a mapping, any keys that are not declared as
        attributes on `projected_type` cause an error.
    """

    def __init__(
        self,
        projected_type: type[T],
        **options: Unpack[PyclassProjectorOptions],
    ) -> None:
        options = options or {}

        object_def = getattr(projected_type, '__dict__', None)
        if options.get('from_attributes', False) and not object_def:
            raise ValueError(
                f'Cannot use `from_attributes` option with {projected_type!r} '
                'because it has no __dict__ attribute defined. Fix: set '
                '`from_attributes` False or ensure the projected_type '
                'does not use slots that hide a __dict__.',
            )

        self.projected_type = projected_type
        self.default_options: PyclassProjectorOptions = options

    def get_attributes(self) -> list[str]:
        """
        Return the attribute names to project into the target type.

        Default implementation uses `__annotations__`. Subclasses (e.g.
        DataclassProjector) can override this to integrate better with
        their target type semantics.
        """
        annotations = getattr(self.projected_type, '__annotations__', {})
        return list(annotations.keys())

    @property
    def from_attributes(self) -> bool:
        return self.default_options.get('from_attributes', False)

    @property
    def exclude_none(self) -> bool:
        return self.default_options.get('exclude_none', False)

    @property
    def forbid_extra(self) -> bool:
        return self.default_options.get('forbid_extra', False)

    def _build_kwargs(
        self,
        obj: Any,
        *,
        from_attributes: bool,
        exclude_none: bool,
        forbid_extra: bool,
    ) -> dict[str, Any]:
        field_names = self.get_attributes()
        field_set = set(field_names)

        if from_attributes:
            source: dict[str, Any] = {}
            for name in field_names:
                if hasattr(obj, name):
                    source[name] = getattr(obj, name)
        else:
            if not hasattr(obj, 'items'):
                raise TypeError(
                    f'{self.__class__.__name__} expected a mapping-like object '
                    f'when `from_attributes=False`, got {type(obj)!r}',
                )
            source = dict(obj)  # type: ignore[arg-type]

            if forbid_extra:
                extra_keys = set(source.keys()) - field_set
                if extra_keys:
                    extras = ', '.join(sorted(map(str, extra_keys)))
                    raise TypeError(
                        f'Unexpected fields for {self.projected_type.__name__}: {extras}',
                    )

        kwargs: dict[str, Any] = {}
        for name in field_names:
            if name not in source:
                continue
            value = source[name]
            if exclude_none and value is None:
                continue
            kwargs[name] = value

        return kwargs

    def __call__(
        self,
        obj: Any,
        **overrides: Unpack[PyclassProjectorOptions],
    ) -> T:
        """
        Project `obj` into an instance of `projected_type`.

        Parameters
        ----------
        obj :
            Source object. Depending on `from_attributes`, this is either
            treated as a mapping (dict-like) or as an attribute container.

        overrides :
            Per-call overrides for `from_attributes`, `exclude_none`,
            and `forbid_extra`.
        """
        options: PyclassProjectorOptions = {
            **self.default_options,
            **overrides,
        }

        from_attributes = options.get('from_attributes', False)
        exclude_none = options.get('exclude_none', False)
        forbid_extra = options.get('forbid_extra', False)

        kwargs = self._build_kwargs(
            obj,
            from_attributes=from_attributes,
            exclude_none=exclude_none,
            forbid_extra=forbid_extra,
        )

        return self.projected_type(**kwargs)  # type: ignore[call-arg]


class DataclassProjector[T](PyclassProjector[T]):
    """
    Specialization of `PyclassProjector` for dataclasses.

    Differences from the base implementation:

    * Verifies that `projected_type` is a dataclass.
    * Uses `dataclasses.fields` to determine the field names instead of
      relying on `__annotations__` alone (handles default / kw-only fields
      more robustly).

    All options (`from_attributes`, `exclude_none`, `forbid_extra`) are
    supported and behave identically to `PyclassProjector`.
    """

    def __init__(
        self,
        projected_type: type[T],
        **options: Unpack[PyclassProjectorOptions],
    ) -> None:
        if not is_dataclass(projected_type):
            raise TypeError(
                f'DataclassProjector requires a dataclass type, got {projected_type!r}',
            )
        super().__init__(projected_type, **options)

    def get_attributes(self) -> list[str]:
        """
        Use `dataclasses.fields` to determine the dataclass field names.

        This respects kw-only and defaulted fields in the dataclass definition.
        """
        return [f.name for f in dataclass_fields(self.projected_type)]  # type: ignore[return-value]



class PydanticProjector[M: BaseModel](Projector[M]):
    """
    Generic projector for Pydantic models.
    It constructs `model_type` instances using `model_validate` (pydantic v2)
    or `parse_obj` (pydantic v1), with configurable behavior via options.

    Options
    -------
    Options passed at initialization are used as defaults for each call,
    but can be overridden per-call.
    strict :
        If True, enable strict type checking during validation.
    extra :
        How to treat extra fields not declared on the model.
    from_attributes :
        * True  -> read attributes from `obj` via `getattr`
        * False -> treat `obj` as a mapping (e.g. dict / RowMapping)
    context :
        Arbitrary context data to pass into the validation process.
    by_alias :
        If True, populate model fields by alias names where defined.
    by_name :
        If True, populate model fields by name.
    """
    def __init__(
        self,
        model_type: type[M],
        **options: Unpack[PydanticModelValidateOptions],
    ) -> None:
        self.pydantic_v2 = hasattr(model_type, 'model_validate')
        self.model_type = model_type
        self.default_options = options or {}

    def __call__(
        self,
        obj: Any,
        **overrides: Unpack[PydanticModelValidateOptions],
    ) -> M:
        if not self.pydantic_v2:
            if overrides:
                warnings.warn(
                    'Passing options to PydanticProjector in pydantic v1 does nothing '
                    'since `parse_obj` is used which accepts no arguments. '
                    'In order to use options, upgrade to pydantic v2 since `parse_obj` '
                    'is deprecated in favor of `model_validate` which accepts options.',
                    DeprecationWarning,
                    stacklevel=2,
                )
            return self.model_type.parse_obj(obj)

        options: dict[str, Any] = {
            **self.default_options,
            **overrides,
        }

        return self.model_type.model_validate(
            obj,
            strict=options.get('strict'),
            extra=options.get('extra'),
            from_attributes=options.get('from_attributes'),
            context=options.get('context'),
            by_alias=options.get('by_alias'),
            by_name=options.get('by_name'),
        )
