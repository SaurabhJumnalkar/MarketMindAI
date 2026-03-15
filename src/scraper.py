import asyncio
import httpx
import logging
import sys
from typing import Optional, List, Dict
from bs4 import BeautifulSoup
from src.database import Database

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

User_Agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"


class Targeted_NewsScraper:
    def __init__(self):
        self.headers = {"User-Agent": "Mozilla/5.0"}

    async def fetch_data(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        # Fetch data and return raw data text
        try:
            response = await client.get(
                url, headers=self.headers, timeout=10.0, follow_redirects=True
            )
            if response.status_code == 200:
                return response.text
            return None

        except Exception as e:
            logger.error(f"Connection Error: {e}")
            return None

    # Parsing the Headlines
    def parse_headlines_rss(self, xml_data: str, ticker: str) -> List[Dict[str, str]]:
        soup = BeautifulSoup(xml_data, "xml")
        results = []

        items = soup.findAll("item")

        for item in items:
            title = item.title.text if item.title else "NO title"
            link = item.link.text if item.link else "NO link"
            results.append({"ticker": ticker, "title": title, "link": link})

        logger.info(f"extracted {len(results)} specific headlines for {ticker}.")
        return results

    async def run(self, tickers: list[str]):
        # Checked for Database
        await Database.init_db()

        async with httpx.AsyncClient() as session:
            for ticker in tickers:
                logger.info(f"Fetching Target headlines for {ticker}...")

                rss_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US"  # noqa: E231

                raw_xml = await self.fetch_data(session, rss_url)
                if raw_xml:
                    clean_data = self.parse_headlines_rss(raw_xml, ticker)

                    print("\n__SNEAK PEEK INTO HEADLINE___\n")
                    for item in clean_data[0:3]:
                        print(item)

                    for item in clean_data:
                        await Database.save_news(
                            item["ticker"], item["title"], item["link"]
                        )


if __name__ == "__main__":
    TARGET_STOCKS = ["AAPL", "TSLA", "NVDA"]

    scraper = Targeted_NewsScraper()
    asyncio.run(scraper.run(TARGET_STOCKS))
