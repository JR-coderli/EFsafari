"""
Redis cache service for API responses.
"""
import json
import hashlib
import logging
from typing import Optional, Any
from functools import wraps

logger = logging.getLogger(__name__)

# Global Redis client
_redis_client = None


def get_redis():
    """Get global Redis client instance."""
    return _redis_client


def init_redis(redis_config: dict):
    """Initialize Redis client from config."""
    global _redis_client
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


def get_cache(key: str) -> Optional[Any]:
    """Get value from cache."""
    client = get_redis()
    if not client:
        return None
    try:
        value = client.get(key)
        if value:
            return json.loads(value)
    except Exception as e:
        logger.debug(f"Cache get failed: {e}")
    return None


def set_cache(key: str, value: Any, ttl: int = 60) -> bool:
    """Set value in cache with TTL."""
    client = get_redis()
    if not client:
        return False
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


def cached(prefix: str = "", ttl: int = 60, exclude_params: list = None):
    """Decorator for caching function results.

    Args:
        prefix: Cache key prefix
        ttl: Time to live in seconds
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

            # Try getting from cache
            cached_value = get_cache(key)
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
