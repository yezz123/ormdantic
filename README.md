# PydanticORM

Asynchronous ORM that uses pydantic models to represent database tables.

## TODO

- [x] Add support for multiple databases.
  - [x] Thanks to [SQLAlchemy](https://www.sqlalchemy.org/), for providing a way
        to connect to multiple databases.
- [x] Creating Tables Based on Pydantic.
  - [x] Function to Get Columns from Pydantic Model.
    - [x] Function to Get Columns Type ex. String, Integer, Boolean, DateTime,
          etc.
  - [x] Function to Get a Many to Many Column.
- [x] Build SQL Queries using [PyPika](https://pypi.org/project/pypika/), a
      Python API for building SQL queries.
  - [x] SQL Functions:
    - [x] Find, Find Many, Find One.
    - [x] Insert, Update, Upsert, Delete.
- [x] Build the Wrapper
- [x] Build Functions to get `Columns` & `Relationships`.
- [ ] Documentation for the whole ORM.
- [ ] Add Tests Case to test different scenarios.
- [ ] Refactor Code to make it more readable.
- [ ] Release the Package.
- [x] Fully Support for Type Annotations.
      [![Lint and Format](https://github.com/yezz123/PydanticORM/actions/workflows/lint.yml/badge.svg)](https://github.com/yezz123/PydanticORM/actions/workflows/lint.yml)

## Development 🚧

### Setup environment 📦

You should create a virtual environment and activate it:

```bash
python -m venv venv/
```

```bash
source venv/bin/activate
```

And then install the development dependencies:

```bash
# Install Flit
pip install flit

# Install dependencies
flit install --symlink
```

### Run tests 🌝

You can run all the tests with:

```bash
make test
```

### Format the code 🍂

Execute the following command to apply `pre-commit` formatting:

```bash
make lint
```

## License

This project is licensed under the terms of the MIT license.
