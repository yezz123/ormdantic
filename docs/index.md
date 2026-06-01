---
hide:
  - navigation
---

<section class="brand-hero">
  <p class="brand-eyebrow">Rust-backed ORM for Pydantic applications</p>
  <h1>Ormdantic</h1>
  <p>Define tables with Pydantic v2 models. Let Rust handle SQL compilation, native execution, relationship planning, and hydration.</p>
  <p>
    <a class="brand-button" href="installation/">Install Ormdantic</a>
    <a class="brand-button brand-button-secondary" href="examples/basic-crud/">View Examples</a>
  </p>
</section>

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

Ormdantic is a library for interacting with asynchronous <abbr title='Also called "Relational databases"'>SQL databases</abbr> from Python code, with Python objects. It is designed to be intuitive, explicit, and robust.

**Ormdantic** is powered by Rust SQL compilation and native Rust database execution, uses <a href="https://docs.pydantic.dev/" class="external-link" target="_blank">Pydantic</a> models, and is built for applications that want a small Python facade over a native runtime.

<div class="brand-grid">
  <div class="brand-card">
    <h3>Pydantic-first tables</h3>
    <p>Use Pydantic models for validation, serialization, and ORM table declarations.</p>
  </div>
  <div class="brand-card">
    <h3>Native Rust runtime</h3>
    <p>Rust owns SQL compilation, bind ordering, native execution, and row hydration.</p>
  </div>
  <div class="brand-card">
    <h3>Async-safe loading</h3>
    <p>Relationship loading is explicit, predictable, and designed for async applications.</p>
  </div>
</div>

## Supported Runtimes

SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle are compiled into the default Python extension.

## License

This project is licensed under the terms of the MIT license.
