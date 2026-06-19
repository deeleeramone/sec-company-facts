from __future__ import annotations

import functools
import threading
import time
from typing import Any, Callable

_HASH_TTL = 10.0
_MAX_ENTRIES = 1024

_cache_lock = threading.Lock()
_cache: dict[Any, Any] = {}
_cache_hash: str | None = None

_hash_lock = threading.Lock()
_hash_value: str | None = None
_hash_checked_at: float = 0.0


def _dolt_hash() -> str:
    global _hash_value, _hash_checked_at
    now = time.monotonic()
    if _hash_value is not None and now - _hash_checked_at < _HASH_TTL:
        return _hash_value
    with _hash_lock:
        now = time.monotonic()
        if _hash_value is not None and now - _hash_checked_at < _HASH_TTL:
            return _hash_value
        try:
            from sec_app.db.backend import connect_read

            sess = connect_read()
            try:
                row = sess.execute("SELECT dolt_hashof_db()").fetchone()
            finally:
                sess.close()
            _hash_value = str(row[0]) if row and row[0] is not None else "unknown"
        except Exception:
            _hash_value = _hash_value or "unknown"
        _hash_checked_at = now
        return _hash_value


def dolt_cached(fn: Callable) -> Callable:
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        global _cache_hash
        h = _dolt_hash()
        key = (fn.__module__, fn.__qualname__, args, tuple(sorted(kwargs.items())))
        with _cache_lock:
            if h != _cache_hash:
                _cache.clear()
                _cache_hash = h
            if key in _cache:
                return _cache[key]
        result = fn(*args, **kwargs)
        with _cache_lock:
            if _cache_hash == h:
                if len(_cache) >= _MAX_ENTRIES:
                    _cache.clear()
                _cache[key] = result
        return result

    return wrapper
