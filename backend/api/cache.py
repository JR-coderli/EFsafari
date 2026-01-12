"""
Redis cache service for API responses with async refresh.

Caching can be disabled via:
1. Redis not available (automatic fallback)
2. enabled: false in config
3. Set CACHE_ENABLED=false environment variable

Async Refresh Strategy:
- Cache TTL: 10 minutes
- Refresh before expiry: 2 minutes
- When user fetches cached data at minute 8, background task refreshes it
- User gets fast response, cache extends to minute 20
"""
import json
import hashlib
import logging
import os
import threading
import time
from typing import Optional, Any, Callable
from functools import wraps

logger = logging.getLogger(__name__)

# Global Redis client
_redis_client = None
# Cache can be disabled via environment variable or config
_cache_enabled = True
# Async refresh settings
_refresh_before_expiry = 120  # seconds
_data_ttl = 600  # seconds


def get_redis():
    """Get global Redis client instance."""
    return _redis_client


def is_cache_enabled() -> bool:
    """Check if caching is enabled."""
    return _cache_enabled and _redis_client is not None


def init_redis(redis_config: dict):
    """Initialize Redis client from config."""
    global _redis_client, _cache_enabled, _refresh_before_expiry, _data_ttl

    # Read config values
    _refresh_before_expiry = redis_config.get("refresh_before_expiry", 120)
    _data_ttl = redis_config.get("data_ttl", 600)

    # Check if cache is explicitly disabled
    cache_enabled_config = redis_config.get("enabled", True)
    cache_env = os.getenv("CACHE_ENABLED", "true").lower()

    if not cache_enabled_config or cache_env == "false":
        logger.info("Caching is disabled via configuration")
        _cache_enabled = False
        _redis_client = None
        return

    try:
        import redis
        _redis_client = redis.Redis(
            host=redis_config.get("host", "localhost"),
            port=redis_config.get("port", 6379),
            db=redis_config.get("db", 0),
            password=redis_config.get("password"),
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2
        )
        # Test connection
        _redis_client.ping()
        logger.info(f"Redis connected: {redis_config.get('host')}:{redis_config.get('port')}")
        logger.info(f"Cache TTL: {_data_ttl}s, Refresh before expiry: {_refresh_before_expiry}s")
    except Exception as e:
        logger.warning(f"Redis connection failed: {e}. Cache will be disabled.")
        _redis_client = None


def cache_key(prefix: str, *args, **kwargs) -> str:
    """Generate a cache key from parameters."""
    # Create a deterministic key from arguments
    key_parts = [prefix]
    for arg in args:
        if arg is not None:
            key_parts.append(str(arg))
    for k, v in sorted(kwargs.items()):
        if v is not None:
            key_parts.append(f"{k}:{v}")

    key_string = ":".join(key_parts)
    # Hash if key is too long
    if len(key_string) > 200:
        key_hash = hashlib.md5(key_string.encode()).hexdigest()[:12]
        return f"{prefix}:{key_hash}"
    return key_string.replace(" ", "_")


def _get_ttl(client, key: str) -> Optional[int]:
    """Get remaining TTL for a key in seconds."""
    try:
        return client.ttl(key)
    except:
        return None


def _should_refresh(ttl: Optional[int]) -> bool:
    """Check if cache should be refreshed based on remaining TTL."""
    if ttl is None or ttl < 0:
        return False
    return ttl <= _refresh_before_expiry


def _refresh_cache_async(key: str, refresher: Callable, ttl: int):
    """Run cache refresh in background thread."""
    def refresh_worker():
        try:
            start = time.time()
            new_value = refresher()
            elapsed = time.time() - start

            if new_value is not None:
                set_cache(key, new_value, ttl)
                logger.debug(f"Async cache refresh completed for {key[:50]}... in {elapsed:.2f}s")
        except Exception as e:
            logger.debug(f"Async cache refresh failed for {key[:50]}...: {e}")

    thread = threading.Thread(target=refresh_worker, daemon=True)
    thread.start()


def get_cache(key: str, refresher: Optional[Callable] = None) -> Optional[Any]:
    """Get value from cache. Returns None if caching is disabled.

    Args:
        key: Cache key
        refresher: Optional async refresh function to call if cache is near expiry
    """
    if not is_cache_enabled():
        return None
    client = get_redis()
    if not client:
        return None
    try:
        value = client.get(key)
        if value:
            parsed = json.loads(value)

            # Check if we should trigger async refresh
            if refresher:
                ttl = _get_ttl(client, key)
                if _should_refresh(ttl):
                    # Trigger async refresh without blocking
                    _refresh_cache_async(key, refresher, _data_ttl)

            return parsed
    except Exception as e:
        logger.debug(f"Cache get failed: {e}")
    return None


def set_cache(key: str, value: Any, ttl: Optional[int] = None) -> bool:
    """Set value in cache with TTL. Does nothing if caching is disabled.

    Args:
        key: Cache key
        value: Value to cache
        ttl: Time to live in seconds (uses data_ttl if not specified)
    """
    if not is_cache_enabled():
        return False
    client = get_redis()
    if not client:
        return False
    if ttl is None:
        ttl = _data_ttl
    try:
        serialized = json.dumps(value, default=str)
        client.setex(key, ttl, serialized)
        return True
    except Exception as e:
        logger.debug(f"Cache set failed: {e}")
        return False


def delete_cache(pattern: str) -> int:
    """Delete keys matching a pattern."""
    client = get_redis()
    if not client:
        return 0
    try:
        keys = client.keys(pattern)
        if keys:
            return client.delete(*keys)
    except Exception as e:
        logger.debug(f"Cache delete failed: {e}")
    return 0


def cached(prefix: str = "", ttl: Optional[int] = None, exclude_params: list = None):
    """Decorator for caching function results.

    Args:
        prefix: Cache key prefix
        ttl: Time to live in seconds (uses data_ttl if not specified)
        exclude_params: Parameters to exclude from cache key
    """
    exclude_params = exclude_params or []

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Skip cache if explicitly requested
            if kwargs.pop("skip_cache", False):
                return func(*args, **kwargs)

            # Build cache key
            cache_kwargs = {k: v for k, v in kwargs.items() if k not in exclude_params}
            key = cache_key(prefix or func.__name__, *args, **cache_kwargs)

            # Try getting from cache (with async refresh capability)
            cached_value = get_cache(key, refresher=lambda: func(*args, **cache_kwargs))
            if cached_value is not None:
                return cached_value

            # Execute function and cache result
            result = func(*args, **kwargs)
            if result is not None:
                set_cache(key, result, ttl)
            return result

        return wrapper
    return decorator


def clear_data_cache():
    """Clear all data-related cache entries."""
    delete_cache("data:*")
    delete_cache("aggregate:*")
    delete_cache("hierarchy:*")
    logger.info("Data cache cleared")
