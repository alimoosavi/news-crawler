import asyncio
import logging

from collectors.tgju_collector import TGJUPriceCollector
from config import settings
from redis_cache_manager import RedisCacheManager

logging.basicConfig(level=logging.INFO)


# ---------------------------
# Async update loop
# ---------------------------
async def update_tgju_cache(collector: TGJUPriceCollector, interval: int = 20):
    while True:
        try:
            collector.collect_prices()
            logging.info("TGJU cache updated successfully")
        except Exception as e:
            logging.error(f"Error updating TGJU cache: {e}")
        await asyncio.sleep(interval)


# ---------------------------
# Main entry point
# ---------------------------
def main():
    cache_manager = RedisCacheManager(host=settings.redis.host,
                                      port=settings.redis.port,
                                      db=settings.redis.db)
    collector = TGJUPriceCollector(cache_manager=cache_manager)
    asyncio.run(update_tgju_cache(collector, interval=20))


if __name__ == "__main__":
    main()
