from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from ormdantic._introspect import (
    contains_list_annotation,
    first_model_arg,
    is_dict_annotation,
    is_list_annotation,
    model_field,
    model_fields,
)


class IntrospectedFlavor(BaseModel):
    id: UUID
    name: str = Field(max_length=63)
    code: str = Field(pattern=r"^[A-Z]{2}$")
    price: Decimal = Field(max_digits=12, decimal_places=2)


class IntrospectedCoffee(BaseModel):
    id: UUID
    flavor: IntrospectedFlavor | UUID
    tags: list[str]
    metadata: dict[str, Any] | None = None


def test_model_fields_wrap_pydantic_v2_field_info() -> None:
    fields = model_fields(IntrospectedFlavor)

    assert set(fields) == {"id", "name", "code", "price"}
    assert fields["id"].annotation is UUID
    assert fields["name"].max_length == 63
    assert fields["code"].pattern == r"^[A-Z]{2}$"
    assert fields["price"].max_digits == 12
    assert fields["price"].decimal_places == 2
    assert fields["name"].required


def test_model_field_detects_container_annotations() -> None:
    tags = model_field(IntrospectedCoffee, "tags")
    metadata = model_field(IntrospectedCoffee, "metadata")

    assert is_list_annotation(tags.annotation)
    assert contains_list_annotation(tags.annotation)
    assert is_dict_annotation(metadata.args[0])
    assert metadata.nullable


def test_first_model_arg_walks_unions_and_containers() -> None:
    assert (
        first_model_arg(
            IntrospectedCoffee.model_fields["flavor"].annotation,
            {IntrospectedFlavor},
        )
        is IntrospectedFlavor
    )
