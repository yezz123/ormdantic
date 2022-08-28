# Frequently Asked Questions 🍂

![Logo](https://raw.githubusercontent.com/yezz123/ormdantic/main/.github/logo.png)

## What is the purpose of this project?

- **Ormdantic** is based on [Pypika](https://github.com/kayak/pypika), and powered by <a href="https://pydantic-docs.helpmanual.io/" class="external-link" target="_blank">Pydantic</a> and <a href="https://sqlalchemy.org/" class="external-link" target="_blank">SQLAlchemy</a>, and Highly inspired by <a href="https://github.com/tiangolo/Sqlmodel" class="external-link" target="_blank">Sqlmodel</a>, Created by [@tiangolo](https://github.com/tiangolo).

## What are the key differences between Ormdantic and Sqlmodel?

| (as of August 2022) | SQLModel | Ormdantic |
|---|---|---|
| Multimorphism  (being both a pydantic model  and a SQLAlchemy model) | Inherits from pydantic BaseModel and uses a modified SQLAlchemy base model  as its meta-class. | Uses a wrapper/decorator to make pydantic BaseModels behave like database tables as well. |
| Database connection/async | Database connection is not built into the main class (SQLModel),  but handled by separate equivalents of SQLAlchemy classes and functions  (Session and create_engine). <br> <br> Since the async equivalents are not  available in `sqlmodel` yet,  one has to use the original  SQLAlchemy ext versions,  which is certainly possible,  but can be a bit cumbersome  until SQLAlchemy 2.0 is released  (type checking is also a problem). | Takes a connection string as a constructor parameter  and creates sessions internally where needed. <br> <br> It is "async by default" one might say. |
| API/Syntax | You typically interact  with both a Session object  and a SQLModel object, e.g.:  <br><br><pre><code>hero = Hero()</code><br><code>session.add(hero)</code><br><code>await session.commit()</code></pre><br>  It uses the SQLAlchemy syntax. | You interact with the database abstraction:  <br><br><pre><code>hero = Hero()</code><br><code>await database["Hero"].insert(hero)</code></pre><br>  The syntax is similar to `PonyORM`, which means you can write queries  to the database using Python generator expressions and lambdas.  It makes the structure similar to writing SQL queries  but even easier to understand and integrate. |
| Background | A bit more established  and has the benefit of  being authored by the creator of FastAPI,  so on paper it should be the ORM to use with FastAPI. Unfortunately said creator has a lot on his plate, so `sqlmodel` may not publish new releases  as often as other projects do. | Brand new and thus unproven,  but could be an exciting alternative. |

Thanks for [@iron3oxide](https://github.com/iron3oxide) for the great explanation!

## How to Support Project?

You can financially support the author (me) through
[![](https://img.shields.io/static/v1?label=Sponsor&message=%E2%9D%A4&logo=GitHub&color=%23fe8e86)](https://github.com/sponsors/yezz123) Paypal
sponsors</a>.

There you could buy me a [coffee ☕️](https://www.buymeacoffee.com/tahiri) to
say thanks. 😄

And you can also become a Silver or Gold sponsor for Ormdantic. 🏅🎉
