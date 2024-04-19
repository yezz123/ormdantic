# Generator Usage

We introduce a new feature called `Generator`, which is a way to generate a Model instance with random data.

So, Given a Pydantic model type can generate instances of that model with randomly generated values.

using `ormdantic.generator.Generator` to generate a Model instance.

```python
from enum import auto, Enum
from uuid import UUID

from ormdantic.generator import Generator
from pydantic import BaseModel


class Flavor(Enum):
    MOCHA = auto()
    VANILLA = auto()


class Brand(BaseModel):
    brand_name: str


class Coffee(BaseModel):
    id: UUID
    description: str
    cream: bool
    sweetener: int
    flavor: Flavor
    brand: Brand


print(Generator(Coffee))
```

so the results will be:

```shell
id=UUID('93b517c2-083b-457d-a0e5-6e1bd2a927e4')
description='ctWOb' cream=True sweetener=234
flavor=<Flavor.VANILLA: 2> brand=Brand(brand_name='LMrIf')
```

We can integrate this with our database while testing our application (Live Tests).
