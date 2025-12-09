from __future__ import annotations

from typing import Any


def copy_object_with[O](
    obj: O,
    **kwargs: Any,
) -> O:
    """
    Create a copy of an object with updated attributes.

    Parameters
    ----------
    obj : O
        The object to copy.
    **kwargs
        Attributes to update in the copied object.

    Returns
    -------
    O
        A new instance of the object's type with updated attributes.
    """
    obj_cls = type(obj)
    if not hasattr(obj, '__slots__'):
        return obj_cls(**{**obj.__dict__, **kwargs})


    slot_attrs = {slot: getattr(obj, slot) for slot in obj.__slots__}
    slot_attrs.update(kwargs)
    obj_cls = type(obj)
    return obj_cls(**slot_attrs)

def object_to_dict(
    obj: Any,
    **extras: Any,
) -> dict[str, Any]:
    """
    Create a dictionary snapshot of an object's attributes.

    Parameters
    ----------
    obj : Any
        The object to snapshot.
    **extras
        Additional key-value pairs to include in the resulting dictionary.

    Returns
    -------
    dict[str, Any]
        A dictionary representation of the object's attributes.
    """
    if not hasattr(obj, '__slots__'):
        data = dict(obj.__dict__)
    else:
        data = {slot: getattr(obj, slot) for slot in obj.__slots__}

    if extras:
        data.update(extras)

    return data




