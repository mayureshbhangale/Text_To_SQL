"""
Schema cache with disk persistence.

T1 introspection is expensive — it hits the DB every time.
This cache stores the result keyed by connection string hash
in a JSON file on disk so it survives process restarts.

Invalidation is manual: call invalidate() or delete the cache file
when you know the schema has changed. There is no TTL by default
because schema changes are infrequent and user-controlled.

Design: the cache is an in-process singleton backed by a JSON file.
For multi-process deployments, swap the backend to Redis using the
same interface.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

# Default location: .schema_cache/ next to wherever the process runs.
# Override by passing cache_dir to SchemaCache().
_DEFAULT_CACHE_DIR = Path(".schema_cache")


class SchemaCache:
    """
    Disk-backed cache for T1 schema output.

    Each connection string gets its own JSON file named by the hash of
    the connection string so credentials are never written to disk.

    Usage:
        cache = SchemaCache()
        cached = cache.get(conn_str)
        if cached is None:
            schema = introspect(conn_str)
            cache.set(conn_str, schema)

    To force a re-introspection after a schema change:
        cache.invalidate(conn_str)   # deletes the cached file
    """

    def __init__(self, cache_dir: Path | str = _DEFAULT_CACHE_DIR) -> None:
        self._cache_dir = Path(cache_dir)

    def _key(self, connection_string: str) -> str:
        """Hash the connection string — credentials are never written to disk."""
        return hashlib.sha256(connection_string.encode()).hexdigest()[:16]

    def _path(self, key: str) -> Path:
        return self._cache_dir / f"{key}.json"

    def get(self, connection_string: str) -> Any | None:
        path = self._path(self._key(connection_string))
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None

    def set(self, connection_string: str, value: Any) -> None:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        path = self._path(self._key(connection_string))
        path.write_text(json.dumps(value, indent=2), encoding="utf-8")

    def invalidate(self, connection_string: str) -> None:
        path = self._path(self._key(connection_string))
        path.unlink(missing_ok=True)

    def clear(self) -> None:
        if self._cache_dir.exists():
            for f in self._cache_dir.glob("*.json"):
                f.unlink(missing_ok=True)

    @property
    def size(self) -> int:
        if not self._cache_dir.exists():
            return 0
        return sum(1 for _ in self._cache_dir.glob("*.json"))


# Module-level singleton — import and use directly
schema_cache = SchemaCache(
    cache_dir=Path(os.environ.get("SCHEMA_CACHE_DIR", ".schema_cache"))
)
