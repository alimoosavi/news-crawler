import redis
import json
import logging
from typing import Any

class RedisCacheManager:
    def __init__(self, host: str, port: int, db: int = 0, prefix: str = "tgju:"):
        """
        Redis cache manager.

        Args:
            host: Redis host
            port: Redis port
            db: Redis database number
            prefix: Prefix to use for all keys
        """
        self.redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.prefix = prefix

    def _full_key(self, key: str) -> str:
        """Internal method to prepend the prefix to the key"""
        return f"{self.prefix}{key}"

    def set(self, key: str, value: Any, expire: int = None):
        """
        Set a key in Redis. Serializes value as JSON if it's a dict or list.

        Args:
            key: Key name
            value: Value to store
            expire: Expiration time in seconds (optional)
        """
        full_key = self._full_key(key)
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        try:
            self.redis.set(full_key, value, ex=expire)
            logging.info(f"Set key {full_key} in Redis")
        except Exception as e:
            logging.error(f"Error setting key {full_key} in Redis: {e}")

    def get(self, key: str) -> Any:
        """
        Get a key from Redis and deserialize JSON if needed.
        """
        full_key = self._full_key(key)
        try:
            value = self.redis.get(full_key)
            if value is None:
                return None
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        except Exception as e:
            logging.error(f"Error getting key {full_key} from Redis: {e}")
            return None

    def set_many(self, data: dict, expire: int = None):
        """
        Set multiple keys in Redis from a dict of key -> value.
        """
        for key, value in data.items():
            self.set(key, value, expire=expire)
