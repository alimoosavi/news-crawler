#!/usr/bin/env python3
"""
Historical Embedding Scheduler

Processes historical news from PostgreSQL database and generates embeddings.
Uses embedding provider configured in .env (OpenAI or Ollama).
"""
import logging
import time
from database_manager import DatabaseManager
from vector_db_manager import VectorDBManager
from config import settings

logger = logging.getLogger("HistoricalEmbeddingScheduler")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class EmbeddingScheduler:
    def __init__(
        self,
        db_manager: DatabaseManager,
        vector_manager: VectorDBManager,
        batch_size: int = 20,
        poll_interval: int = 30
    ):
        self.db_manager = db_manager
        self.vector_manager = vector_manager
        self.batch_size = batch_size
        self.poll_interval = poll_interval

    def run_forever(self):
        logger.info("=" * 80)
        logger.info("Historical Embedding Scheduler Started")
        logger.info(f"Provider: {settings.embedding.provider}")
        logger.info(f"Model: {settings.embedding.openai_model if settings.embedding.provider == 'openai' else settings.embedding.ollama_model}")
        logger.info(f"Dimension: {settings.embedding.embedding_dim}")
        logger.info(f"Batch Size: {self.batch_size}")
        logger.info("=" * 80)
        
        while True:
            # Fetch pending news
            news_batch = self.db_manager.get_pending_news_batch(limit=self.batch_size)
            if not news_batch:
                logger.info("No pending news. Sleeping...")
                time.sleep(self.poll_interval)
                continue

            logger.info(f"📥 Fetched {len(news_batch)} pending news")

            # Persist into Qdrant
            try:
                inserted = self.vector_manager.persist_news_batch(news_batch)
                logger.info(f"✅ Inserted {inserted} items into Qdrant")

                # Mark as COMPLETED
                if inserted > 0:
                    links = [n.link for n in news_batch]
                    updated = self.db_manager.mark_news_completed(links)
                    logger.info(f"✅ Marked {updated} news as COMPLETED")
                else:
                    logger.warning("⚠️  No items inserted")
                    
            except Exception as e:
                logger.error(f"❌ Error processing batch: {e}")
                time.sleep(5)
                continue

            time.sleep(2)


def main():
    """Main entry point"""
    
    # Initialize managers
    try:
        db_manager = DatabaseManager(settings.database)
        logger.info("✅ DatabaseManager initialized")
    except Exception as e:
        logger.critical(f"❌ Failed to initialize DatabaseManager: {e}")
        return
    
    try:
        vector_manager = VectorDBManager(
            qdrant_config=settings.qdrant,
            embedding_config=settings.embedding,
            logger=logger
        )
        logger.info(f"✅ VectorDBManager ready with {settings.embedding.provider}")
    except Exception as e:
        logger.critical(f"❌ Failed to initialize VectorDBManager: {e}")
        return
    
    # Start scheduler
    scheduler = EmbeddingScheduler(
        db_manager=db_manager,
        vector_manager=vector_manager,
        batch_size=20,
        poll_interval=30
    )
    
    try:
        scheduler.run_forever()
    except KeyboardInterrupt:
        logger.info("\n🛑 Scheduler stopped by user")
    except Exception as e:
        logger.critical(f"❌ Scheduler crashed: {e}")
        raise


if __name__ == "__main__":
    main()