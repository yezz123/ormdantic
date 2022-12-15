"""Unit tests for model generator."""
import datetime
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
    multiple_of_int_ge: int = Field(multiple_of=7, ge=1000)
    multiple_of_int_gt: int = Field(multiple_of=7, gt=-1000)
    multiple_of_int_le: int = Field(multiple_of=7, le=1000)
    multiple_of_int_lt: int = Field(multiple_of=7, lt=-1000)
    range_int_multiple_of: int = Field(lt=200, gt=101, multiple_of=11)
    range_int: int = Field(le=200, ge=101)
    time: datetime.datetime


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


def test_datetime() -> None:
    """Test datetime."""
    model = Generator(Coffee)
    assert isinstance(model.time, datetime.datetime)
