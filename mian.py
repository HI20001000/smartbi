"""Compatibility entrypoint.

Please run with: uvicorn app.main:app --reload
"""

from app.main import app

__all__ = ["app"]
