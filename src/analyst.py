import asyncio
import logging
import sys
from transformers import pipeline
import aiosqlite

# from src.database import Database


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class SentimentAnalyst:
    def __init__(self):
        logger.info("Loading FINBERT Model... (Will take some time...)")

        self.analyzer = pipeline("sentiment-analysis", model="ProsusAI/finbert")
        logger.info("Model Loaded Successfully.")

    async def analyze_db_headlines(self):
        # Analyze headlines with no score assigned
        async with aiosqlite.connect("marketmind.db") as db:
            # Step 1 : Fetch headlines with no score
            async with db.execute(
                "SELECT id,Title FROM news WHERE sentiment_score IS NULL"
            ) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                logger.info("No new Headdline analyzed.")
                return

            logger.info(f"Analyzing {len(rows)} new Headlines...")

            # Step 2 : Analyze (Batch Processing)

            updates = []
            for row in rows:
                row_id, text = row

                result = self.analyzer(text)[0]

                label = result["label"]
                confidence = result["score"]

                final_score = confidence if label == "positive" else -confidence

                if label == "neutral":
                    final_score = 0.0

                updates.append((final_score, label, row_id))

            # Step 3 : Update the Database
            await db.executemany(
                "UPDATE news SET sentiment_score=?,sentiment_label=? WHERE id=?",
                updates,
            )

            await db.commit()
            logger.info("Analysis completed.Database updated.")


if __name__ == "__main__":
    analyst = SentimentAnalyst()
    asyncio.run(analyst.analyze_db_headlines())
