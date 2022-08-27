# Frequently Asked Questions 🍂

![Logo](https://raw.githubusercontent.com/yezz123/ormdantic/main/.github/logo.png)

## What is the purpose of this project?

- **Ormdantic** is based on [Pypika](https://github.com/kayak/pypika), and powered by <a href="https://pydantic-docs.helpmanual.io/" class="external-link" target="_blank">Pydantic</a> and <a href="https://sqlalchemy.org/" class="external-link" target="_blank">SQLAlchemy</a>, and Highly inspired by <a href="https://github.com/tiangolo/Sqlmodel" class="external-link" target="_blank">Sqlmodel</a>, Created by [@tiangolo](https://github.com/tiangolo).

## What are the key differences between Ormdantic and Sqlmodel?

- `ormdantic` uses a wrapper/decorator to make pydantic BaseModels behave like database tables as well, whereas the model class of `sqlmodel` inherits from pydantic BaseModel and uses a modified SQLAlchemy base model as its meta-class to achieve the same thing.

- In `sqlmodel`, the database connection is not built into the main class (SQLModel), but handled by separate equivalents of SQLAlchemy classes and functions (Session and create_engine).

  Since the async equivalents are not available in `sqlmodel` yet, one has to use the original SQLAlchemy ext versions, which is certainly possible, but can be a bit cumbersome until SQLAlchemy 2.0 is released (type checking is also a problem).

  `ormdantic` takes a connection string as a constructor parameter instead and creates sessions internally where needed. It is "async by default" one might say.

- The aforementioned difference leads to another difference:
  
  In `sqlmodel`, you typically interact with both a Session object and a SQLModel object, e.g.:

    ```python
        ...
        hero = Hero()   # Hero inherits from SQLModel
        session.add(hero)
        await session.commit()
        ...
    ```

  In `ormdantic`, you just interact with the database abstraction:

    ```python
        ...
        hero = Hero()   # Hero inherits from pydantic BaseModel and is decorated as database table
        await database["Hero"].insert(hero)
        ...
    ```

    Some people find the latter to be a bit more elegant/intuitive, but that's of course subjective.

- `ormdantic` adds pypika to the mix, `sqlmodel` does not seem to include it or anything similar, but that's more of an internal difference. It is an added dependency though for those who care about it.

- Another difference is of course that `sqlmodel` is a bit more established and has the benefit of being authored by the creator of FastAPI, so on paper it should be the ORM to use with FastAPI.

  Unfortunately said creator has a lot on his plate and `sqlmodel` hasn't seen a new release for some time now. `ormdantic` on the other hand is of course brand new and thus unproven, but could be an exciting alternative.

- `ormdantic` is similar to `PonyORM` in that it enables youto write queries to the database using Python generator expressions and lambdas. It makes the structure similar to writing SQL queries but even easier to understand and integrate.

Thanks for [@iron3oxide](https://github.com/iron3oxide) for the great explanation!

## How to Support Project?

You can financially support the author (me) through
[![](https://img.shields.io/static/v1?label=Sponsor&message=%E2%9D%A4&logo=GitHub&color=%23fe8e86)](https://github.com/sponsors/yezz123) Paypal
sponsors</a>.

There you could buy me a [coffee ☕️](https://www.buymeacoffee.com/tahiri) to
say thanks. 😄

And you can also become a Silver or Gold sponsor for Ormdantic. 🏅🎉
