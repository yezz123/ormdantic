"""Shared Ormdantic database target for the Todo example."""

from importlib import import_module

from ormdantic import Ormdantic

from .config import load_settings

settings = load_settings()
db = Ormdantic(settings.database_url)

# The CLI/Playground imports this module directly, so registration must happen here.
import_module(".models", package=__package__)
