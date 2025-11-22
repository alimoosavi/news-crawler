import logging
import time
from database_manager import DatabaseManager
from vector_db_manager import VectorDBManager
from config import settings

logger = logging.getLogger("EmbeddingScheduler")
logging.basicConfig(level=logging.INFO)

class EmbeddingScheduler:
    def __init__(self, db_manager: DatabaseManager, vector_manager: VectorDBManager, batch_size: int = 20, poll_interval: int = 30):
        self.db_manager = db_manager
        self.vector_manager = vector_manager
        self.batch_size = batch_size
        self.poll_interval = poll_interval

    def run_forever(self):
        logger.info("Embedding scheduler started...")
        while True:
            # 1. Fetch pending news
            news_batch = self.db_manager.get_pending_news_batch(limit=self.batch_size)
            if not news_batch:
                logger.info("No pending news. Sleeping...")
                time.sleep(self.poll_interval)
                continue

            logger.info(f"Fetched {len(news_batch)} pending news for embedding.")

            # 2. Persist into Qdrant
            inserted = self.vector_manager.persist_news_batch(news_batch)
            logger.info(f"Inserted {inserted} items into Qdrant.")

            # 3. Mark them as COMPLETED in DB
            if inserted > 0:
                links = [n.link for n in news_batch]
                updated = self.db_manager.mark_news_completed(links)
                logger.info(f"Marked {updated} news as COMPLETED in DB.")

            # 4. Short pause before next fetch
            time.sleep(2)


def main():
    db_manager = DatabaseManager(settings.database)
    vector_manager = VectorDBManager(settings.qdrant, settings.openai, logger)
    scheduler = EmbeddingScheduler(db_manager, vector_manager)
    scheduler.run_forever()


if __name__ == "__main__":
    main()
