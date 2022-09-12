from typing import Type

import sqlalchemy


class ConfigurationError(Exception):
    """Raised for mal-configured database models or schemas."""

    def __init__(self, msg: str):
        super().__init__(msg)


class UndefinedBackReferenceError(ConfigurationError):
    """Raised when a back reference is missing from a table."""

    def __init__(self, table_a: str, table_b: str, field: str) -> None:
        super().__init__(
            f'Many relation defined on "{table_a}.{field}" to table {table_b}" must be'
            f' defined with a back reference on "{table_a}".'
        )


class MismatchingBackReferenceError(ConfigurationError):
    """Raised when a back reference is typed incorrectly."""

    def __init__(
        self, table_a: str, table_b: str, field: str, back_reference: str
    ) -> None:
        super().__init__(
            f'Many relation defined on "{table_a}.{field}" to'
            f' "{table_b}.{back_reference}" must use the same model type'
            f" back-referenced."
        )


class MustUnionForeignKeyError(ConfigurationError):
    """Raised when a relation field doesn't allow for just foreign key."""

    def __init__(
        self, table_a: str, table_b: str, field: str, model_b: Type, pk_type: Type  # type: ignore
    ) -> None:
        super().__init__(
            f'Relation defined on "{table_a}.{field}" to "{table_b}" must be a union'
            f' type of "Model | model_pk_type" e.g. "{model_b.__name__} | {pk_type}"'
        )


class TypeConversionError(ConfigurationError):
    """Raised when a Python type fails to convert to SQL."""

    def __init__(self, type: Type) -> None:  # type: ignore
        super().__init__(
            f"Type {type} is not supported by SQLAlchemy {sqlalchemy.__version__}."
        )
