# === MODULE PURPOSE ===
# Web UI for trading confirmations.
# Provides FastAPI-based API and simple HTML pages.

from src.web.app import create_app

__all__ = ["create_app"]
