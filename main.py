import logging
from typing import List, Dict

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from crawlers import IRNACrawler, ISNACrawler

app = FastAPI()

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NewsRequest(BaseModel):
    links: List[str]  # List of news links to fetch


@app.post("/fetch_news/irna/")
async def fetch_irna_news(request: NewsRequest) -> Dict[str, Dict[str, str]]:
    """Fetch news content asynchronously."""
    crawler = IRNACrawler(logger, request.links)
    await crawler.run()
    return crawler.get_news()


@app.post("/fetch_news/isna/")
async def fetch_isna_news(request: NewsRequest) -> Dict[str, Dict[str, str]]:
    """Fetch news content asynchronously."""
    crawler = ISNACrawler(logger, request.links)
    await crawler.run()
    return {link: content for link, content in crawler.get_news().items() if content is not None}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=6001)
