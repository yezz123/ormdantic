---
hide:
  - navigation
---

![Logo](https://raw.githubusercontent.com/yezz123/ormdantic/main/.github/logo.png)

<p align="center">
    <em>Asynchronous ORM that uses pydantic models to represent database tables ✨</em>
</p>

<p align="center">
<a href="https://github.com/yezz123/ormdantic/actions/workflows/ci.yml" target="_blank">
    <img src="https://github.com/yezz123/ormdantic/actions/workflows/ci.yml/badge.svg" alt="Test">
</a>
<a href="https://codecov.io/gh/yezz123/ormdantic">
    <img src="https://codecov.io/gh/yezz123/ormdantic/branch/main/graph/badge.svg"/>
</a>
<a href="https://pypi.org/project/ormdantic" target="_blank">
    <img src="https://img.shields.io/pypi/v/ormdantic?color=%2334D058&label=pypi%20package" alt="Package version">
</a>
<a href="https://github.com/sponsors/yezz123" target="_blank">
    <img src="https://img.shields.io/static/v1?label=Sponsor&message=%E2%9D%A4&logo=GitHub&color=%23fe8e86" alt="Sponsor">
</a>
</p>

Ormdantic is a library for interacting with Asynchronous <abbr title='Also called "Relational databases"'>SQL databases</abbr> from Python code, with Python objects. It is designed to be intuitive, easy to use, compatible, and robust.

**Ormdantic** is powered by a Rust SQL compiler, <a href="https://pydantic-docs.helpmanual.io/" class="external-link" target="_blank">Pydantic</a>, and <a href="https://sqlalchemy.org/" class="external-link" target="_blank">SQLAlchemy</a> async execution, and is highly inspired by <a href="https://github.com/tiangolo/Sqlmodel" class="external-link" target="_blank">Sqlmodel</a>, Created by [@tiangolo](https://github.com/tiangolo).

The key features are:

* **Easy to use**: It has sensible defaults and does a lot of work underneath to simplify the code you write.
* **Compatible**: It combines SQLAlchemy async execution, Pydantic models, and Rust query compilation to simplify the code you write as much as possible, allowing you to reduce the code duplication to a minimum, but while getting the best developer experience possible.
* **Extensible**: You have SQLAlchemy's async execution layer and Ormdantic's Rust internals underneath.
* **Short Queries**: You can write queries in a single line of code, and it will be converted to the appropriate syntax for the database you are using.

## License

This project is licensed under the terms of the MIT license.
