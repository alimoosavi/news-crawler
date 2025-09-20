import redis


class CacheManager:
    def __init__(self, host="localhost", port=6379, db=0):
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True  # 🔑 makes .get() return str instead of bytes
        )

    def get_last_link(self, source: str) -> str | None:
        """Retrieve the last published link for a given source"""
        return self.redis_client.get(source)

    def update_last_link(self, source: str, last_link: str):
        """Update the last published link for a given source"""
        self.redis_client.set(source, last_link)
