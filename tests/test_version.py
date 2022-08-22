import pydantic_orm


def test_version() -> None:
    assert pydantic_orm.__version__ == "1.0.0"
