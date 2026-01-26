import aiosqlite
import logging

logger = logging.getLogger(__name__)
DB_NAME = "marketmind.db"


# Architect
class Database:
    @staticmethod
    async def init_db():
        # Connect the file if it doen't exist then create
        async with aiosqlite.connect(DB_NAME) as db:
            # SQL Command
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS news (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            title TEXT UNIQUE,
                            link TEXT,
                            published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                            """
            )

            # Save Changes
            await db.commit()

    @staticmethod
    async def save_news(title: str, link: str):
        try:
            async with aiosqlite.connect(DB_NAME) as db:
                # SQL Command
                await db.execute(
                    "INSERT OR IGNORE INTO news (title,link) VALUES (?,?)",
                    (title, link),
                )
                await db.commit()
                logger.info(f"Saved: {title[:30]}...")  # Log first 30 chars

        except Exception as e:
            logger.error(f"DB Error: {e}")


# Test BLOCK

if __name__ == "__main__":
    import asyncio

    async def test_run():
        # Initialize

        await Database.init_db()

        # Save A fake headline
        await Database.save_news("Apple Stock HIts $300", "https://fake.url")

        # Verify
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT * FROM news") as cursor:
                rows = await cursor.fetchall()
                print("\n--- DATABASE CONTENTS ---")

                for row in rows:
                    print(row)

    asyncio.run(test_run())
