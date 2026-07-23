FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential curl pkg-config \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --profile minimal \
    && rm -rf /var/lib/apt/lists/*
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /src
COPY pyproject.toml uv.lock Cargo.toml README.md LICENSE ./
COPY ormdantic ./ormdantic
COPY rust ./rust
RUN uv build --wheel --out-dir /dist

FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_ENV=production
WORKDIR /app
COPY --from=builder /dist /dist
RUN python -m pip install --no-cache-dir \
        /dist/ormdantic-*.whl \
        "fastapi>=0.116,<1" \
        "uvicorn[standard]>=0.35,<1" \
    && useradd --create-home --uid 10001 app
COPY --chown=app:app examples/todo_app/ /app/
USER app
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
