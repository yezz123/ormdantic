from ormdantic.generator._crud import OrmCrud as CRUD
from ormdantic.generator._lazy import generate as Generator
from ormdantic.generator._table import OrmTable as Table

__all__ = ["Table", "CRUD", "Generator"]
