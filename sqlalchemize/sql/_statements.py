from __future__ import annotations

import abc
import functools
import inspect
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, ParamSpec, Self, TypeVar, overload

from sqlalchemy import Delete, Insert, Select, Update

type Statement = Insert | Update | Delete | Select


class ParameterizationError(Exception):
    """Raised when a statement template cannot be parameterized with the given arguments.

    This exception is raised when required parameters are missing during statement
    template parameterization. It provides details about which parameters were
    expected and which were provided.
    """

    def __init__(
        self,
        passed_parameters: dict[str, Any],
        required_params: set[str],
        statement: Statement,
    ) -> None:
        self.passed_parameters = passed_parameters
        self.required_params = required_params
        self.statement = statement
        super().__init__(
            f'Failed to parameterize statement {statement!r}. Ensure all required '
            f'parameters are provided. Required: {sorted(required_params)}, '
            f'Provided: {sorted(passed_parameters.keys())}, '
            f'Missing: {sorted(self.missing_parameters)}'
        )

    @property
    def missing_parameters(self) -> set[str]:
        """Get the set of missing parameters required for parameterization.

        Returns
        -------
        set[str]
            A set of missing parameter names.
        """
        return self.required_params - self.passed_parameters.keys()


class StatementTemplate[S: Statement, V: Any](abc.ABC):
    """Abstract base class for statement templates.

    A statement template encapsulates either a SQLAlchemy statement with bind
    parameters or a factory function that creates statements. Templates can be
    parameterized to create executable statements and support partial application.

    Parameters
    ----------
    value : V
        The underlying value (statement or factory function).
    """

    def __init__(self, value: V) -> None:
        self._value = value

    @abc.abstractmethod
    def get_parameters(self) -> set[str]:
        """Get the set of parameter names required by this statement template.

        Returns
        -------
        set[str]
            A set of parameter names required by this statement template.
        """
        ...

    @abc.abstractmethod
    def build(self, **params: Any) -> S:
        """Build a parameterized statement from the template.

        Parameters
        ----------
        **params : Any
            The parameters to use for parameterization.

        Returns
        -------
        S
            The parameterized SQLAlchemy statement.

        Raises
        ------
        ParameterizationError
            If required parameters are missing.
        """
        ...

    @abc.abstractmethod
    @classmethod
    def infer_type_of(cls, value: V) -> type[S]:
        """Infer the statement type from the template value.

        Parameters
        ----------
        value : V
            The template value to infer from.

        Returns
        -------
        type[S]
            The inferred statement type.
        """
        ...

    @abc.abstractmethod
    def partial(self, **params: Any) -> Self:
        """Create a new statement template with some parameters pre-applied.

        This allows for progressive parameterization of templates, similar to
        functools.partial.

        Parameters
        ----------
        **params : Any
            The parameters to pre-apply.

        Returns
        -------
        Self
            A new statement template with the given parameters pre-applied.
        """
        ...

    def validate_parameters(self, **params: Any) -> None:
        """Validate that all required parameters are provided.

        Parameters
        ----------
        **params : Any
            The parameters to validate.

        Raises
        ------
        ParameterizationError
            If required parameters are missing.
        """
        required_parameters = self.get_parameters()
        if not required_parameters.issubset(params.keys()):
            raise ParameterizationError(
                passed_parameters=params,
                required_params=required_parameters,
                statement=self._value,  # type: ignore[arg-type]
            )


class BindparamTemplate[S: Statement](StatementTemplate[S, S]):
    """Template for statements using SQLAlchemy bindparam() parameters.

    This template handles SQLAlchemy statements that use bindparam() for
    parameterization. It extracts parameter names from the compiled statement
    and applies them using the params() method.

    Parameters
    ----------
    value : S
        The SQLAlchemy statement with bind parameters.
    _default_arguments : dict[str, Any] | None, optional
        Pre-applied default parameters for partial application.
    """

    def __init__(
        self,
        value: S,
        *,
        _default_arguments: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(value)
        self._default_arguments = _default_arguments or {}

    def get_parameters(self) -> set[str]:
        """Extract parameter names from the compiled statement.

        Returns
        -------
        set[str]
            Parameter names found in the statement's bind parameters.
        """
        compiled = self._value.compile()
        return set(compiled.params.keys())

    def build(self, **params: Any) -> S:
        """Build the statement by applying parameters.

        Parameters
        ----------
        **params : Any
            The parameters to bind to the statement.

        Returns
        -------
        S
            The parameterized statement.

        Raises
        ------
        ParameterizationError
            If required parameters are missing.
        """
        combined_params = {**self._default_arguments, **params}
        self.validate_parameters(**combined_params)
        return self._value.params(**combined_params)

    @classmethod
    def infer_type_of(cls, value: S) -> type[S]:
        """Infer the statement type directly from the value.

        Parameters
        ----------
        value : S
            The statement to infer from.

        Returns
        -------
        type[S]
            The type of the statement.
        """
        return type(value)  # type: ignore[return-value]

    def partial(self, **params: Any) -> BindparamTemplate[S]:
        """Create a new template with additional pre-applied parameters.

        Parameters
        ----------
        **params : Any
            Additional parameters to pre-apply.

        Returns
        -------
        BindparamTemplate[S]
            A new template with combined default parameters.
        """
        combined_params = {**self._default_arguments, **params}
        return BindparamTemplate(self._value, _default_arguments=combined_params)


class FactoryTemplate[S: Statement](StatementTemplate[S, Callable[..., S]]):
    """Template for factory functions that create statements.

    This template wraps a callable that returns a SQLAlchemy statement. It
    introspects the function signature to determine required parameters and
    supports partial application.

    Parameters
    ----------
    value : Callable[..., S]
        A factory function that creates a statement.
    """

    def get_parameters(self) -> set[str]:
        """Extract parameter names from the factory function signature.

        Returns
        -------
        set[str]
            Parameter names from the function signature.
        """
        signature = inspect.signature(self._value)
        return set(signature.parameters.keys())

    def build(self, **params: Any) -> S:
        """Build a statement by calling the factory function.

        Parameters
        ----------
        **params : Any
            Arguments to pass to the factory function.

        Returns
        -------
        S
            The created statement.

        Raises
        ------
        ParameterizationError
            If required parameters are missing.
        """
        self.validate_parameters(**params)
        return self._value(**params)

    @classmethod
    def infer_type_of(cls, value: Callable[..., S]) -> type[S]:
        """Infer the statement type from the factory's return annotation.

        Parameters
        ----------
        value : Callable[..., S]
            The factory function to infer from.

        Returns
        -------
        type[S]
            The return type annotation of the factory.

        Raises
        ------
        TypeError
            If the factory lacks a return type annotation.
        """
        return_type = inspect.signature(value).return_annotation
        if return_type is inspect.Signature.empty:
            raise TypeError(
                f'Cannot infer statement type from factory function without return '
                f'annotation: {value!r} (function: {value.__name__})'
            )

        return return_type  # type: ignore[return-value]

    def partial(self, **params: Any) -> FactoryTemplate[S]:
        """Create a new template with pre-applied parameters.

        Parameters
        ----------
        **params : Any
            Parameters to pre-apply to the factory function.

        Returns
        -------
        FactoryTemplate[S]
            A new template wrapping a partial function.
        """
        partial_factory = functools.partial(self._value, **params)
        return FactoryTemplate(partial_factory)


def get_statement_template[S: Statement](
    statement: S | Callable[..., S],
) -> StatementTemplate[S, Any]:
    """Create an appropriate statement template for the given value.

    Automatically selects between BindparamTemplate and FactoryTemplate based
    on whether the value is a statement or a callable.

    Parameters
    ----------
    statement : S | Callable[..., S]
        The SQLAlchemy statement or factory function.

    Returns
    -------
    StatementTemplate[S, Any]
        The corresponding statement template.
    """
    if inspect.isfunction(statement) or inspect.ismethod(statement):
        return FactoryTemplate(statement)  # type: ignore[arg-type]

    return BindparamTemplate(statement)  # type: ignore[arg-type]


def get_statement_template_cls(
    statement: Statement | Callable[..., Statement],
) -> type[StatementTemplate]:
    """Get the appropriate statement template class for the given value.

    Parameters
    ----------
    statement : Statement | Callable[..., Statement]
        The SQLAlchemy statement or factory function.

    Returns
    -------
    type[StatementTemplate]
        The corresponding statement template class (BindparamTemplate or FactoryTemplate).
    """
    if inspect.isfunction(statement) or inspect.ismethod(statement):
        return FactoryTemplate

    return BindparamTemplate


class StatementRepository[S: Statement]:
    """Registry for statement templates of a specific type.

    A repository manages a collection of named statement templates, all of which
    produce the same type of SQLAlchemy statement (Select, Insert, Update, or Delete).

    Parameters
    ----------
    templates : dict[str, StatementTemplate] | None, optional
        Initial templates to register.
    statement_type : type[S] | None, optional
        The type of statements this repository handles. If not provided,
        must be set as a class attribute.

    Attributes
    ----------
    statement_type : type[S]
        The type of statements this repository manages.
    """

    statement_type: type[S]

    def __init__(
        self,
        templates: dict[str, StatementTemplate] | None = None,
        *,
        statement_type: type[S] | None = None,
    ) -> None:
        if statement_type is not None:
            self.statement_type = statement_type

        if not hasattr(self, 'statement_type') or self.statement_type is None:
            raise TypeError(
                'StatementRepository requires a statement_type to be defined either '
                'as a class attribute or passed during initialization.'
            )

        self._templates: dict[str, StatementTemplate] = templates or {}

    @overload
    def register(
        self,
        key: str,
        template: Callable[..., S],
        *,
        exists_okay: bool = False,
    ) -> FactoryTemplate[S]: ...

    @overload
    def register(
        self,
        key: str,
        template: S,
        *,
        exists_okay: bool = False,
    ) -> BindparamTemplate[S]: ...

    def register(
        self, key: str, template: S | Callable[..., S], *, exists_okay: bool = False
    ) -> StatementTemplate:
        """Register a new statement template.

        Parameters
        ----------
        key : str
            Unique identifier for the template.
        template : S | Callable[..., S]
            The statement or factory function to register.
        exists_okay : bool, optional
            If ``True``, allows overwriting an existing template with the
            same key.

        Returns
        -------
        StatementTemplate
            The created template wrapper.

        Raises
        ------
        KeyError
            If a template with the given key already exists.
        """
        if key in self._templates and not exists_okay:
            raise KeyError(f'Statement template with key {key!r} already exists')

        template_cls = get_statement_template_cls(template)
        self._templates[key] = template_cls(template)  # type: ignore[arg-type]
        return self._templates[key]

    def get_template(self, key: str) -> StatementTemplate[S, Any]:
        """Retrieve a statement template by key.

        Parameters
        ----------
        key : str
            The template identifier.

        Returns
        -------
        StatementTemplate[S, Any]
            The requested template.

        Raises
        ------
        KeyError
            If no template exists with the given key.
        """
        try:
            template = self._templates[key]
        except KeyError as exc:
            raise KeyError(f'No statement template found for key {key!r}') from exc

        if not isinstance(template, StatementTemplate):
            raise TypeError(
                f'Statement template for key {key!r} is not a valid '
                f'StatementTemplate instance: {template!r}'
            )

        return template  # type: ignore[return-value]

    def register_all(self, templates: dict[str, S | Callable[..., S]]) -> None:
        """Register multiple templates at once.

        Parameters
        ----------
        templates : dict[str, S | Callable[..., S]]
            A mapping of keys to statements or factory functions.

        Raises
        ------
        KeyError
            If any key already exists in the repository.
        """
        for key, template in templates.items():
            self.register(key, template)

    def exists(
        self, key: str, *, is_template_type: type[StatementTemplate] | None = None
    ) -> bool:
        """Check if a statement template with the given key exists.

        Parameters
        ----------
        key : str
            The key identifying the statement template.
        is_template_type : type[StatementTemplate] | None, optional
            If provided, also checks if the template is of the specified type.

        Returns
        -------
        bool
            ``True`` if the template exists (and matches the type if provided),
            otherwise ``False``.
        """
        template = self._templates.get(key)
        if template is None:
            return False

        if is_template_type is not None:
            return isinstance(template, is_template_type)

        return True

    def __contains__(self, key: str) -> bool:
        """Check if a statement template with the given key exists.

        Parameters
        ----------
        key : str
            The key identifying the statement template.

        Returns
        -------
        bool
            ``True`` if the template exists, otherwise ``False``.
        """
        return key in self._templates


def _infer_type_of_template(
    template: Statement | Callable[..., Statement],
) -> type[Statement]:
    """Infer the statement type from a template value.

    Parameters
    ----------
    template : Statement | Callable[..., Statement]
        The template value to infer from.

    Returns
    -------
    type[Statement]
        The inferred statement type.
    """
    if inspect.isfunction(template) or inspect.ismethod(template):
        return FactoryTemplate.infer_type_of(template)  # type: ignore[arg-type]

    return BindparamTemplate.infer_type_of(template)  # type: ignore[arg-type]


class SelectRepository(StatementRepository[Select]):
    """Repository specialized for SELECT statements."""

    statement_type = Select


class InsertRepository(StatementRepository[Insert]):
    """Repository specialized for INSERT statements."""

    statement_type = Insert


class UpdateRepository(StatementRepository[Update]):
    """Repository specialized for UPDATE statements."""

    statement_type = Update


class DeleteRepository(StatementRepository[Delete]):
    """Repository specialized for DELETE statements."""

    statement_type = Delete


P = ParamSpec('P')
T = TypeVar('T')


def _validate_factory_template_function(
    return_type: inspect.Signature.empty | type[Statement],
    func_name: str,
) -> None:
    if return_type is inspect.Signature.empty:
        raise TypeError(
            'Cannot register factory function as statement template without a return '
            f'type annotation for `{func_name}`.'
        )

    if not issubclass(return_type, (Select, Insert, Update, Delete)):  # type: ignore[arg-type]
        raise TypeError(
            'Cannot register factory function as statement template with invalid '
            f'return type annotation `{return_type}` for `{func_name}`.'
        )


@dataclass(slots=True)
class SQLTemplateRegistry:
    """Central registry for all SQL statement templates.

    This class provides a unified interface for managing statement templates
    across all SQLAlchemy statement types. It automatically routes templates
    to the appropriate repository based on their inferred type.

    Examples
    --------
    >>> templates = SQLTemplates()
    >>> # Register a factory function
    >>> @templates.register("get_user")
    >>> def get_user(user_id: int) -> Select:
    ...     return select(User).where(User.id == user_id)
    >>> # Build a statement
    >>> stmt = templates.build(Select, name='get_user', user_id=123)
    """

    insert: InsertRepository = field(default_factory=InsertRepository, init=False)
    update: UpdateRepository = field(default_factory=UpdateRepository, init=False)
    delete: DeleteRepository = field(default_factory=DeleteRepository, init=False)
    select: SelectRepository = field(default_factory=SelectRepository, init=False)

    def get_repository_for_type(
        self, stmt_type: type[Statement]
    ) -> StatementRepository:
        statement_name = stmt_type.__name__.lower()
        return getattr(self, statement_name)  # type: ignore[return-value]

    def register_template(
        self,
        key: str,
        template: Statement | Callable[..., Statement],
        *,
        exists_ok: bool = False,
    ) -> StatementTemplate:
        """Add a new statement template to the appropriate repository.

        Parameters
        ----------
        key : str
            Unique identifier for the template.

        template : Statement | Callable[..., Statement]
            The statement or factory function to register.

        exists_ok : bool, optional
            If ``True``, allows overwriting existing templates with the same keys
            by default false.

        Returns
        -------
        StatementTemplate
            The created template wrapper.
        """
        stmt_type = _infer_type_of_template(template)
        repo = self.get_repository_for_type(stmt_type)
        return repo.register(key, template, exists_okay=exists_ok)

    def update_templates(
        self,
        templates: dict[str, Statement | Callable[..., Statement]],
        *,
        exists_ok: bool = True,
    ) -> None:
        """Add multiple statement templates to their respective repositories.

        Parameters
        ----------
        templates : dict[str, Statement | Callable[..., Statement]]
            A mapping of keys to statements or factory functions.

        exists_ok : bool, optional
            If ``True``, allows overwriting existing templates with the same keys
            by default true.
        """
        for key, template in templates.items():
            self.register_template(key, template, exists_ok=exists_ok)

    def get_template[S: Statement](
        self,
        key: str,
        *,
        stmt_type: type[S] | None = None,
    ) -> StatementTemplate[S, Any] | None:
        """Retrieve a statement template by type and/or key.

        Parameters
        ----------
        stmt_type : type[S] | None, optional
            The type of statement (Select, Insert, Update, Delete).
        key : str | None, optional
            The template identifier.

        Returns
        -------
        StatementTemplate[S, Any]
            The requested statement template.

        Raises
        ------
        ValueError
            If neither stmt_type nor key is provided.
        KeyError
            If no matching template is found.
        """
        if stmt_type:
            repo = self.get_repository_for_type(stmt_type)
            if key is not None:
                return repo.get_template(key)  # type: ignore[return-value]

        if key:
            for repo in (
                self.select,
                self.insert,
                self.update,
                self.delete,
            ):
                if repo.exists(key):
                    return repo.get_template(key)  # type: ignore[return-value]

        return None

    def build_template[S: Statement](
        self,
        key: str,
        type_: type[S] | None = None,
        **params: Any,
    ) -> S:
        if not (template := self.get_template(key=key, stmt_type=type_)):
            raise KeyError(f'No statement template found for key {key!r}')
        return template.build(**params)

    def register(
        self,
        key: str,
        *,
        exists_okay: bool = False,
    ) -> Callable[[Callable[P, T]], Callable[P, T]]:
        """Decorator for automatically registering a `FactoryTemplate` in the registry
        with the given key and infers the statement type from the function's return
        annotation. If the return type is not annotated or is not a valid and a
        `TypeError` is raised.

        Parameters
        ----------
        key : str
            Unique identifier for the template.

        exists_okay : bool, optional
            If ``True``, allows overwriting an existing template with the
            same key, by default false.

        Returns
        -------
        Callable[[Callable[P, T]], Callable[P, T]]
            A decorator that registers the decorated function as a statement template.
        """

        def decorator(func: Callable[P, T]) -> Callable[P, T]:
            return_type = inspect.signature(func).return_annotation
            _validate_factory_template_function(return_type, func.__name__)
            self.register_template(key, func, exists_ok=exists_okay)  # type: ignore[arg-type]
            return func

        return decorator
