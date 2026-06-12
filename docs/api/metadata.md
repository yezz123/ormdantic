# Metadata Models

Metadata models are Pydantic models used by table decorators, migrations, and reflection.

## Result And Relationship

::: ormdantic.models.Result
::: ormdantic.models.Relationship

## Table Metadata

::: ormdantic.models.TableColumn
::: ormdantic.models.TableIndex
::: ormdantic.models.TableCheck
::: ormdantic.models.TableUnique
::: ormdantic.models.TableForeignKey
::: ormdantic.models.TableExclusion

## Database-Level Metadata

::: ormdantic.models.DatabaseNamespace
::: ormdantic.models.DatabaseSequence
::: ormdantic.models.DatabaseView

## Where They Are Used

```python
from ormdantic import TableColumn, TableIndex, TableUnique


@db.table(
    pk="id",
    columns={"id": TableColumn(identity=True)},
    indexes=[TableIndex(name="flavor_name_idx", columns=["name"])],
    unique_constraints=[TableUnique(name="flavor_name_unique", columns=["name"])],
)
class Flavor(BaseModel):
    id: int
    name: str
```
