import aiosqlite
import asyncio

DB_NAME = "marketmind.db"


async def add_sentiment_column():
    async with aiosqlite.connect(DB_NAME) as db:
        # Sentiment_score = -1,0,1
        # Sentiment_label = negative,neutral,,positive
        try:
            await db.execute("ALTER TABLE news ADD COLUMN sentiment_score REAL")
            await db.execute("ALTER TABLE news ADD COLUMN sentiment_label TEXT")
            await db.commit()
            print("Successfully added sentiment columns in DB.")
        except Exception as e:
            print(f"Migration Failed.: {e}")


if __name__ == "__main__":
    asyncio.run(add_sentiment_column())
