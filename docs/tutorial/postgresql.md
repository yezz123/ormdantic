# Run the application with PostgreSQL

This chapter starts a production-shaped dependency chain: PostgreSQL becomes
healthy, a one-shot container applies the PostgreSQL artifacts, and only then does
the API start.

```mermaid
%% Compose startup
flowchart LR
    P[PostgreSQL 17] -->|service_healthy| M[Migration job]
    M -->|service_completed_successfully| A[FastAPI service]
    A -->|/health ready| U[Client]
```

## Inspect the Compose definition

```yaml
--8<-- "examples/todo_app/docker-compose.yml"
```

No service mounts the Docker socket. PostgreSQL data lives in a named volume, and
the runtime image runs as a non-root user. The image builds the local Ormdantic
wheel in a Rust builder stage, then copies only the wheel and example into a slim
Python runtime stage.

## Start the stack

From the repository root:

```console
docker compose -f examples/todo_app/docker-compose.yml up --build
```

Wait until `api` is healthy, then open <http://127.0.0.1:8000/docs> or run:

```console
curl http://127.0.0.1:8000/health
```

The expected backend is `postgresql`. Stop containers without erasing data:

```console
docker compose -f examples/todo_app/docker-compose.yml down
```

The named volume remains. Add `--volumes` only when you intentionally want to
delete all example data.

The default credentials are demonstration values. Override `POSTGRES_DB`,
`POSTGRES_USER`, and `POSTGRES_PASSWORD` locally; use managed secrets and TLS in a
real deployment. Review [PostgreSQL driver behavior](../drivers/postgresql.md)
before selecting URL options or relying on native schema features.
