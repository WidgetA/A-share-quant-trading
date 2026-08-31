"""Web package with a lazy legacy-app export.

Importing ``src.web.v20_app`` must not import the legacy platform application:
that module imports iQuant and trading route definitions.  Keep compatibility
for ``from src.web import create_app`` without making every web submodule cross
the execution boundary.
"""

from __future__ import annotations

from typing import Any

__all__ = ["create_app"]


def __getattr__(name: str) -> Any:
    if name != "create_app":
        raise AttributeError(name)
    from src.web.app import create_app

    return create_app
