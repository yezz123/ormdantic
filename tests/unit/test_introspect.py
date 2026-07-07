from __future__ import annotations

from decimal import Decimal
from typing import Any, Union
from uuid import UUID

from pydantic import BaseModel, Field

from ormdantic._introspect import (
    FieldMetadata,
    annotation_allows_none,
    contains_list_annotation,
    first_model_arg,
    is_dict_annotation,
    is_list_annotation,
    is_union_annotation,
    model_field,
    model_fields,
    rebuild_model,
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


class IntrospectedDefaults(BaseModel):
    value: str = "dark"
    tags: list[str] = Field(default_factory=list, min_length=1)
    score: int = Field(ge=1, gt=0, le=10, lt=11, multiple_of=1)


def test_model_fields_wrap_pydantic_v2_field_info() -> None:
    fields = model_fields(IntrospectedFlavor)

    assert set(fields) == {"id", "name", "code", "price"}
    assert fields["id"].annotation is UUID
    assert fields["name"].max_length == 63
    assert fields["code"].pattern == r"^[A-Z]{2}$"
    assert fields["price"].max_digits == 12
    assert fields["price"].decimal_places == 2
    assert fields["name"].required
    assert fields["id"].origin is None
    assert fields["id"].args == ()


def test_model_field_detects_container_annotations() -> None:
    tags = model_field(IntrospectedCoffee, "tags")
    metadata = model_field(IntrospectedCoffee, "metadata")

    assert is_list_annotation(tags.annotation)
    assert contains_list_annotation(tags.annotation)
    assert is_dict_annotation(metadata.args[0])
    assert metadata.nullable
    assert annotation_allows_none(type(None))
    assert is_union_annotation(str | None)
    assert is_union_annotation(Union[str, None])
    assert not is_union_annotation(str)


def test_first_model_arg_walks_unions_and_containers() -> None:
    assert (
        first_model_arg(
            IntrospectedCoffee.model_fields["flavor"].annotation,
            {IntrospectedFlavor},
        )
        is IntrospectedFlavor
    )
    assert first_model_arg(["unhashable"], {IntrospectedFlavor}) is None


def test_field_metadata_defaults_constraints_and_rebuild() -> None:
    value = model_field(IntrospectedDefaults, "value")
    tags = model_field(IntrospectedDefaults, "tags")
    score = model_field(IntrospectedDefaults, "score")

    assert value.default == "dark"
    assert tags.default_factory is list
    assert tags.min_items == 1
    assert tags.max_items is None
    assert tags.constraint("missing") is None
    assert score.ge == 1
    assert score.gt == 0
    assert score.le == 10
    assert score.lt == 11
    assert score.multiple_of == 1
    assert contains_list_annotation(dict[str, list[int]])
    assert is_dict_annotation(dict)
    rebuild_model(IntrospectedDefaults)


def test_field_metadata_constraint_reads_direct_attributes_before_metadata() -> None:
    class DirectInfo:
        annotation = str
        default = "vanilla"
        default_factory = None
        metadata = []

        def __init__(self) -> None:
            self.max_length = 12

        def is_required(self) -> bool:
            return False

    class EmptyMetadata:
        min_length = None

    class MetadataInfo:
        annotation = str
        default = None
        default_factory = None
        metadata = [EmptyMetadata()]

        def is_required(self) -> bool:
            return True

    assert FieldMetadata("name", DirectInfo()).constraint("max_length") == 12  # type: ignore[arg-type]
    assert FieldMetadata("name", MetadataInfo()).constraint("min_length") is None  # type: ignore[arg-type]
