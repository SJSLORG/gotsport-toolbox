"""arbiter_etl package init for tests and imports.

Expose a small helper to configure package logging from a repository entrypoint.
"""

from .logging_config import configure_logging  # re-export helper

__all__ = ["arbiter_ops", "configure_logging"]
