"""
Utility functions for the Flexible CLI.

This module provides utility functions for Redis connection management
and other helper functionality. Decorators have been moved to decorators.py.
"""

import logging
from typing import Optional
from redis import Redis
from config import Config

logger = logging.getLogger(__name__)


# Global Redis client instance (singleton pattern)
_redis_client: Optional[Redis] = None


def get_redis_client() -> Redis:
    """
    Get a Redis client instance using singleton pattern.
    
    This function ensures only one Redis connection is created per application
    instance. If Redis is unavailable, it returns a MockRedis client that
    provides the same interface but performs no operations.
    
    Returns:
        Redis: A Redis client instance or MockRedis if connection fails
        
    Raises:
        No exceptions - failures are handled gracefully with MockRedis fallback
        
    Example:
        client = get_redis_client()
        client.set('key', 'value', ex=3600)
        value = client.get('key')
    """
    global _redis_client
    
    if _redis_client is None:
        try:
            _redis_client = Redis(
                host=Config.REDIS_HOST,
                port=Config.REDIS_PORT,
                db=Config.REDIS_DB,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5
            )
            # Test connection to ensure Redis is available
            _redis_client.ping()
            logger.info("Successfully connected to Redis at %s:%d", Config.REDIS_HOST, Config.REDIS_PORT)
        except Exception as e:
            logger.warning(
                "Failed to connect to Redis at %s:%d - %s. Caching will be disabled.",
                Config.REDIS_HOST, Config.REDIS_PORT, e
            )
            # Return a mock Redis client that provides the same interface
            _redis_client = MockRedis()
    
    return _redis_client


class MockRedis:
    """
    Mock Redis client for when Redis is unavailable.
    
    This class provides the same interface as the Redis client but performs
    no actual caching operations. It's used as a fallback when Redis connection
    fails, allowing the application to continue functioning without caching.
    
    All methods return appropriate default values that indicate cache misses
    or successful operations without actually performing any Redis operations.
    """
    
    def get(self, key: str) -> None:
        """Always return None (cache miss)."""
        return None
    
    def set(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """Always return True (simulated success)."""
        return True
    
    def setex(self, key: str, time: int, value: str) -> bool:
        """Always return True (simulated success)."""
        return True
    
    def delete(self, key: str) -> int:
        """Always return 0 (no keys deleted)."""
        return 0
    
    def exists(self, key: str) -> bool:
        """Always return False (key doesn't exist)."""
        return False
    
    def ping(self) -> bool:
        """Always return True (simulated connection)."""
        return True