"""Application version management.

Version is read from package metadata (pyproject.toml).
"""

from functools import lru_cache
from importlib.metadata import version as get_package_version


@lru_cache(maxsize=1)
def get_version() -> str:
    """Get application version from package metadata.

    Returns:
        Version string (e.g., "0.9.0") or "dev" if not installed as package.
    """
    try:
        return get_package_version("detectflow-backend")
    except Exception:
        return "dev"


__version__ = get_version()
