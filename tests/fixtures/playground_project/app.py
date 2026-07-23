from pydantic import BaseModel

from ormdantic import Ormdantic

print("fixture import output")

db = Ormdantic("sqlite:///:memory:")


@db.table("users", pk="id")
class User(BaseModel):
    id: int
    name: str


not_a_database = object()
