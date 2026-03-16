import pandas as pd
import sqlite3
import logging
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class DataMerger:
    def __init__(self, db_path: str = "marketmind.db"):
        self.db_path = db_path

    def load_price_data(self, ticker: str) -> pd.DataFrame:
        file_path = f"data/{ticker}_historical.csv"

        if not os.path.exists(file_path):
            logger.error(f"Price Data not found: {file_path}")
            return pd.DataFrame
        # parse Date column as date column not just text
        df = pd.read_csv(file_path, parse_dates=["Date"])

        # Neturilize timezone
        df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None).dt.normalize()
        return df

    def load_aggregate_sentiment_data(self) -> pd.DataFrame:
        if not os.path.exists(self.db_path):
            logger.error(f"DB Not found {self.db_path}")
            return pd.DataFrame

        conn = sqlite3.connect(self.db_path)
        query = "SELECT ticker,published_at,sentiment_score FROM news WHERE sentiment_score IS NOT NULL"

        df_news = pd.read_sql(query, conn)
        conn.close()

        if df_news.empty:
            logger.warning("No Scored news found in DB.")
            return pd.DataFrame

        # Convert to Date
        df_news["Date"] = (
            pd.to_datetime(df_news["published_at"]).dt.tz_localize(None).dt.normalize()
        )

        # average daily sentiment

        daily_sentiment = pd.DataFrame(
            df_news.groupby(["Date", "ticker"])["sentiment_score"].mean()
        )
        daily_sentiment.rename(
            columns={"sentiment_score": "Daily_sentiment_score"}, inplace=True
        )

        return daily_sentiment

    def run(self, ticker: str):
        logger.info(f"starting Data merger for {ticker}...")

        df_price = self.load_price_data(ticker=ticker)
        df_sentiment = self.load_aggregate_sentiment_data()

        if df_price.empty:
            return

        if not df_sentiment.empty:
            df_sentiment = df_sentiment.reset_index()

            df_sentiment_filtered = df_sentiment[df_sentiment["ticker"] == ticker]
            print(df_sentiment_filtered)
            final_df = pd.merge(
                df_price,
                df_sentiment_filtered[["Date", "Daily_sentiment_score"]],
                on="Date",
                how="left",
            )

            final_df["Daily_sentiment_score"] = final_df[
                "Daily_sentiment_score"
            ].fillna(0.0)

        else:
            final_df = df_price.copy()
            final_df["Daily_sentiment_score"] = 0.0

        # Save master Dataset
        output_path = f"data/{ticker}_master.csv"
        final_df.to_csv(output_path, index=False)
        logger.info(f"Successfully Created Master Dataset for {ticker}.")

        print(f"/n ___Master Dataset sample For {ticker}_____/n")
        print(final_df.tail(5))


if __name__ == "__main__":
    dataMerger = DataMerger()
    dataMerger.run("AAPL")
