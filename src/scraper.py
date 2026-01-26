import asyncio
import httpx
import logging
import sys
from typing import Optional, List, Dict
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

User_Agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"


class NewsScraper:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.headers = {"User-Agent": User_Agent}

    async def fetch_html(self, session: httpx.AsyncClient) -> Optional[str]:
        # Fetch Html and return raw html text

        try:
            logger.info(f"Connecting to {self.base_url}...")

            # async with

            response = await session.get(
                self.base_url, headers=self.headers, timeout=10.0, follow_redirects=True
            )

            if response.status_code == 200:
                html = response.text
                logger.info(f"Downloaded HTML of {len(html)} bytes.")
                return html

            else:
                logger.info(f"Failed.Status Code: {response.status}")
                return None

        except Exception as e:
            logger.error(f"Connection Error: {e}")
            return None

    # Parsing the Headlines
    def parse_headlines(self, html: str) -> List[Dict[str, str]]:
        soup = BeautifulSoup(html, "html.parser")
        results = []

        # yahoo finance Specific tag for headlines
        headlines = soup.find_all("h3")

        for h3 in headlines:
            text = h3.get_text(strip=True)
            if len(text) > 15:
                results.append({"headline": text})
        logger.info(f"Successfully extracted {len(results)} headlines")
        return results

    async def run(self):
        async with httpx.AsyncClient() as session:
            raw_html = await self.fetch_html(session)
            if raw_html:
                clean_data = self.parse_headlines(raw_html)
                print("\n --- SAMPLE HEADLINES ---")
                for item in clean_data[:5]:
                    print(f"-{item['headline']}")
                print(len(clean_data))


if __name__ == "__main__":
    URL = "https://finance.yahoo.com/topic/stock-market-news/"
    scraper = NewsScraper(URL)
    asyncio.run(scraper.run())
