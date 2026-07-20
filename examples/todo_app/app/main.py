"""FastAPI application factory for the Ormdantic Todo example."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ormdantic import Ormdantic

from .database import db
from .errors import TodoApplicationError, error_response
from .routes import router
from .service import TodoService


def create_app(database: Ormdantic | None = None) -> FastAPI:
    """Build an application around an injected or configured database target."""
    runtime_database = database or db

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await runtime_database.init()
        app.state.todo_service = TodoService(runtime_database)
        app.state.database_dialect = runtime_database.runtime_diagnostics()["backend"]
        yield

    application = FastAPI(
        title="Ormdantic Todo API",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.include_router(router)

    @application.exception_handler(TodoApplicationError)
    async def handle_application_error(
        _request: Request,
        error: TodoApplicationError,
    ) -> JSONResponse:
        status_code, payload = error_response(error)
        return JSONResponse(status_code=status_code, content=payload)

    return application


app = create_app()
