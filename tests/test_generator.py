"""Unit tests for model generator."""
import datetime
import types
from collections import OrderedDict
from enum import Enum, auto
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from ormdantic.generator import Generator


class Flavor(Enum):
    """Coffee flavors."""

    MOCHA = auto()
    VANILLA = auto()


class Brand(BaseModel):
    """A coffee brand."""

    brand_name: str


class Coffee(BaseModel):
    """Test model."""

    id: UUID = Field(default_factory=uuid4)
    description: str | None
    cream: bool
    sweetener: int
    flavor: Flavor
    brand: Brand
    volume: float = 3.14
    bagels: list[str]
    list_of_lists: list[list[str]]
    dictionary: dict[str, list[int]]
    union: str | int | list[str]
    multiple_of_float: float = Field(multiple_of=3.14)
    multiple_of_int_ge: int = Field(multiple_of=7, ge=1000)
    multiple_of_int_gt: int = Field(multiple_of=7, gt=-1000)
    multiple_of_int_le: int = Field(multiple_of=7, le=1000)
    multiple_of_int_lt: int = Field(multiple_of=7, lt=-1000)
    range_int_multiple_of: int = Field(lt=200, gt=101, multiple_of=11)
    range_int: int = Field(le=200, ge=101)
    always_none: types.NoneType = None  # type: ignore
    str_constraint_min: str = Field(min_length=101)
    str_constraint_max: str = Field(max_length=200)
    str_constraint_minmax: str = Field(min_length=101, max_length=200)
    date_field: datetime.date
    time_field: datetime.time
    timedelta_field: datetime.timedelta
    datetime_field: datetime.datetime
    not_specifically_supported_type: OrderedDict  # type: ignore


def test_validate() -> None:
    """Test validate."""
    Generator(Coffee)


def test_generator() -> None:
    assert Generator(Coffee).description is not None


def test_none() -> None:
    model = Generator(Coffee, optionals_use_none=True)
    assert model.description is None


def test_use_defaults() -> None:
    id_ = uuid4()
    assert Generator(Coffee, use_default_values=False).id != id_
    assert Generator(Coffee, id=id_).id == id_


def test_use_kwargs() -> None:
    brand = Brand(brand_name=str(uuid4()))
    assert Generator(Coffee, brand=brand).brand == brand


def test_multiple_of_int_ge() -> None:
    """Test multiple_of with ge."""
    model = Generator(Coffee)
    assert model.multiple_of_int_ge % 7 == 0
    assert model.multiple_of_int_ge >= 1000


def test_range_int_multiple_of() -> None:
    """Test range with multiple_of."""
    model = Generator(Coffee)
    assert model.range_int_multiple_of < 200
    assert model.range_int_multiple_of > 101
    assert model.range_int_multiple_of % 11 == 0


def test_range_int() -> None:
    """Test range with multiple_of."""
    model = Generator(Coffee)
    assert model.range_int <= 200
    assert model.range_int >= 101


def test_str_constraint_min() -> None:
    """Test str constraint min_length."""
    model = Generator(Coffee)
    assert len(model.str_constraint_min) >= 101


def test_str_constraint_max() -> None:
    """Test str constraint max_length."""
    model = Generator(Coffee)
    assert len(model.str_constraint_max) <= 200


def test_str_constraint_minmax() -> None:
    """Test str constraint min_length and max_length."""
    model = Generator(Coffee)
    assert len(model.str_constraint_minmax) >= 101
    assert len(model.str_constraint_minmax) <= 200


def test_not_specifically_supported_type() -> None:
    """Test not specifically supported type."""
    model = Generator(Coffee)
    assert isinstance(model.not_specifically_supported_type, OrderedDict)


def test_always_none() -> None:
    """Test always_none."""
    model = Generator(Coffee)
    assert model.always_none is None


def test_multiple_of_int_gt() -> None:
    """Test multiple_of_int_gt."""
    model = Generator(Coffee)
    assert model.multiple_of_int_gt % 7 == 0
    assert model.multiple_of_int_gt > -1000


def test_multiple_of_int_le() -> None:
    """Test multiple_of_int_le."""
    model = Generator(Coffee)
    assert model.multiple_of_int_le % 7 == 0
    assert model.multiple_of_int_le <= 1000
