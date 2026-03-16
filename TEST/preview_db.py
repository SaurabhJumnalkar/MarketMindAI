import asyncio
import aiosqlite


async def read_db():
    async with aiosqlite.connect("marketmind.db") as db:
        # Step 1 : Fetch headlines with no score
        async with db.execute(
            "SELECT id,ticker,sentiment_score REAL, sentiment_label FROM news"
        ) as cursor:
            rows = await cursor.fetchall()

        if not rows:
            print("No new Headdline analyzed.")
            return None

        print("\n ___DB DATA _____\n")
        for row in rows:
            print(row)


if __name__ == "__main__":
    asyncio.run(read_db())
