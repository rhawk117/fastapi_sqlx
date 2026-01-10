import contextlib
import dataclasses as dc
import inspect as pyinspect
from typing import Any, Literal

from sqlalchemy import Row, RowMapping
from sqlalchemy import inspect as sa_inspect

ExtraOption = Literal['allow', 'forbid', 'ignore']

def get_attribute_names(obj: type) -> set[str]:
    if not pyinspect.isclass(obj):
        raise TypeError('obj must be a class type')

    if dc.is_dataclass(obj):
        return {field.name for field in dc.fields(obj)}

    if object_dict := getattr(obj, '__dict__', None):
        return {key for key in object_dict.keys() if not key.startswith('__')}

    if object_slots := getattr(obj, '__slots__', None):
        if isinstance(object_slots, str):
            return {object_slots}

        return set(object_slots)

    return {attr.name for attr in pyinspect.classify_class_attrs(obj)}

def sqlalchemy_to_dict(obj: Any) -> dict[str, Any] | None:
    if isinstance(obj, Row):
        return dict(obj._mapping)

    if isinstance(obj, RowMapping):
        return dict(obj)

    if dc.is_dataclass(obj) and not isinstance(obj, type):
        return dc.asdict(obj)

    try:
        state = sa_inspect(obj)
        if mapper := getattr(state, 'mapper', None):
            return {attr.key: getattr(obj, attr.key) for attr in mapper.column_attrs}
    except Exception:
        pass

    result = None
    with contextlib.suppress(Exception):
        result = dict(vars(obj))

    return result

def validate_kwargs(
    kwargs: dict[str, Any],
    object_attrs: set[str],
    extra: ExtraOption,
) -> None:
    for key in kwargs.keys():
        if key in object_attrs or extra == 'allow':
            continue

        if extra == 'ignore':
            kwargs.pop(key)
            continue

        raise TypeError(f'Unexpected attribute "{key}" for class during coercion')
