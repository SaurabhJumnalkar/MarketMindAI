import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def upload_to_postgres():
    logger.info("Starting Uploading the Data...")

    load_dotenv()
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        logger.error(f"Database URL {db_url} NOt found in .env file.")
        return

    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    # Establish COnnection to supabase

    engine = create_engine(db_url)

    upload_table_list = [
        {"table": "dim_geography", "file": "data/seed/dim_geography.csv"},
        {"table": "dim_business_unit", "file": "data/seed/dim_business_unit.csv"},
        {"table": "dim_date", "file": "data/seed/dim_date.csv"},
        {"table": "dim_keyfigure", "file": "data/seed/dim_keyfigure.csv"},
        {"table": "fact_financials", "file": "data/seed/fact_financials.csv"},
    ]

    start_time = time.time()

    for task in upload_table_list:
        table_name = task["table"]
        file_path = task["file"]

        logger.info(f"Uploading {file_path}...\n")

        try:
            chunk_size = 20000
            total_rows_uploaded = 0

            csv_iterator = pd.read_csv(file_path, chunksize=chunk_size)

            for i, chunk in enumerate(csv_iterator):
                # If it's first iterator then replace table or else append in table
                if_exists_behaviour = "replace" if i == 0 else "append"

                chunk.to_sql(
                    name=table_name,
                    con=engine,
                    if_exists=if_exists_behaviour,
                    index=False,
                )

                total_rows_uploaded += len(chunk)
                logger.info(
                    f"   ✅ Uploaded {total_rows_uploaded:,} rows so far..."  # noqa E231
                )
            logger.info(f"✨ Successfully finished {table_name}!")

        except Exception as e:
            logger.error(f"❌ Failed to upload {table_name}. Error: {e}")
            return

    elapsed_time = round((time.time() - start_time) / 60, 2)
    logger.info("\n========================================")
    logger.info(f"🎉 ENTERPRISE INGESTION COMPLETE in {elapsed_time} minutes!")
    logger.info("========================================")


if __name__ == "__main__":
    upload_to_postgres()
