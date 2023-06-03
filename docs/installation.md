## Requirements

A recent and currently supported version of Python (right now, <a href="https://www.python.org/downloads/" class="external-link" target="_blank">Python supports versions 3.10 and above</a>).

As **Ormdantic** is based on **Pydantic** and **SQLAlchemy** and **Pypika**, it requires them. They will be automatically installed when you install Ormdantic.

## Installation

You can add Ormdantic in a few easy steps. First of all, install the dependency:

```shell
$ pip install ormdantic

---> 100%

Successfully installed Ormdantic
```

* Install The specific Asynchronous ORM library for your database.

### SQLite

```shell
$ pip install ormdantic[sqlite]
```

### PostgreSQL

```shell
$ pip install ormdantic[postgresql]
```
