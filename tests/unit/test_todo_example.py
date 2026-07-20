from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "examples" / "todo_app"


def test_todo_example_has_an_isolated_runtime_extra() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())

    assert project["project"]["optional-dependencies"]["examples"] == [
        "fastapi>=0.116,<1",
        "httpx>=0.28,<1",
        "uvicorn[standard]>=0.35,<1",
    ]
    core_dependencies = [
        dependency.casefold().replace("_", "-")
        for dependency in project["project"]["dependencies"]
    ]
    for example_dependency in ("fastapi", "httpx", "uvicorn"):
        assert not any(
            dependency.startswith(example_dependency)
            for dependency in core_dependencies
        )


def test_todo_example_has_the_approved_package_boundaries() -> None:
    expected = {
        "app/config.py",
        "app/database.py",
        "app/errors.py",
        "app/main.py",
        "app/models.py",
        "app/routes.py",
        "app/schemas.py",
        "app/service.py",
        "docker-compose.yml",
        "Dockerfile",
        "ormdantic.toml",
    }

    actual = {
        path.relative_to(EXAMPLE).as_posix()
        for path in EXAMPLE.rglob("*")
        if path.is_file()
    }

    assert expected <= actual
