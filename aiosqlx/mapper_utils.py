from __future__ import annotations

import fnmatch
import importlib
import pkgutil
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable
    from types import ModuleType

    from sqlalchemy.orm import DeclarativeBase


def iter_modules_under(root_package: str) -> Iterable[str]:
    """
    Iterate fully-qualified module names under the given root package.

    Parameters
    ----------
    root_package : str
        The root package name to start searching from.

    Returns
    -------
    Iterable[str]
        An iterator over fully-qualified module names.

    Yields
    ------
    Iterator[Iterable[str]]
        Fully-qualified module names found under the root package.
    """
    pkg = importlib.import_module(root_package)

    if not hasattr(pkg, '__path__'):
        return

    prefix = pkg.__name__ + '.'
    for module_info in pkgutil.walk_packages(pkg.__path__, prefix):
        yield module_info.name

def _filter_modules_by_glob(
    module_names: Iterable[str],
    pattern: str,
) -> list[str]:
    """
    Filter module names by a glob pattern.

    Parameters
    ----------
    module_names : Iterable[str]
        An iterable of fully-qualified module names.
    pattern : str
        The glob pattern to match against.

    Returns
    -------
    list[str]
        A list of module names that match the given pattern.
    """
    return [name for name in module_names if fnmatch.fnmatch(name, pattern)]



def _extract_root_package(pattern: str) -> str:
    parts = pattern.split('.')
    if not parts:
        raise ImportError(f'Invalid pattern: {pattern!r}')

    root = parts[0]
    if '*' in root or '?' in root or '[' in root:
        raise ImportError(
            f'Root package in pattern must not contain wildcards; got {root!r}'
        )
    return root


def import_module_names(*modules: str) -> list[ModuleType]:
    """
    Import modules by their fully-qualified names.


    """
    imported = []
    for modname in modules:
        module = importlib.import_module(modname)
        imported.append(module)

    return imported


def import_by_glob(pattern: str) -> list[ModuleType]:
    """
    Import modules matching a glob pattern.

    Parameters
    ----------
    pattern : str
        The glob pattern to match module names against.

    Returns
    -------
    list[object]
        A list of imported module objects.
    """
    root = _extract_root_package(pattern)
    all_modules = list(iter_modules_under(root))
    matched = _filter_modules_by_glob(all_modules, pattern)
    return import_module_names(*matched)


def register_sqlalchemy_models(
    base_model: type[DeclarativeBase],
    *,
    pattern: str | None = None,
    orm_modules: list[str] | None = None,
) -> list[str]:
    """
    Register SQLAlchemy models by importing modules.

    Parameters
    ----------
    base_model : type[DeclarativeBase]
        The base class for SQLAlchemy models.

    pattern : str | None, optional
        A glob pattern to match module names for importing models.

    orm_modules : list[ModuleType] | None, optional
        A list of already imported modules containing models.

    Returns
    -------
    list[str]
        A list of names of registered model classes.
    """
    if not (pattern or orm_modules):
        raise ValueError("Either 'pattern' or 'orm_modules' must be provided")

    imported_modules: list[ModuleType] = []
    if pattern:
        imported_modules.extend(import_by_glob(pattern))

    if orm_modules:
        imported_modules.extend(import_module_names(*orm_modules))


    registered_models = []
    for model in base_model.registry.mappers:
        cls = model.class_
        registered_models.append(cls.__name__)

    return registered_models

__all__ = (
    'import_by_glob',
    'import_module_names',
    'register_sqlalchemy_models',
)