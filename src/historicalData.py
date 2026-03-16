import yfinance as yf
import pandas as pd
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class HistoricalDataFetcher:
    def __init__(self):
        # Ensure we've data folder to save our csv files
        os.makedirs("data", exist_ok=True)

    def fetch_stock_data(self, ticker: str, period: str = "2y") -> pd.DataFrame:
        #  Download historical data and 2y means for 2years
        logger.info(f"Downloading Historcal Data of {period} for {ticker}...")

        stock = yf.Ticker(ticker)
        df = stock.history(period=period)

        if df.empty:
            logger.error(f"No data found for {ticker}.")
            return df

        # We'll delete timezone related data to remove confusion for ml models
        df.index = df.index.tz_localize(None)
        return df

    def engineers_features(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Calculating Features needed...")

        # AI needs features for calculating trends
        df["Daily Return"] = df["Close"].pct_change()
        df["MA_7"] = df["Close"].rolling(window=7).mean()
        df["Volatility_14"] = df["Daily Return"].rolling(window=14).std()

        # We'll create a target variable which is the one which we'll predict
        # Means we'll take next day's value and check and put 1 for Up and 0 for Down
        # And our predicted value will be compared with this target variable for knowing accuracy

        df["Target_direction"] = (df["Close"].shift(-1) > df["Close"]).astype(int)

        # We've created checks of volatility of 14 days for every record that means for first 14 records this value will come null
        # So we'll delete firt 14 rows to remove na values

        df = df.dropna()
        return df

    def run(self, ticker: str):
        df = self.fetch_stock_data(ticker)

        if not df.empty:
            df = self.engineers_features(df)

            # save data to our data folder
            file_path = f"data/{ticker}_historical.csv"
            df.to_csv(file_path)
            logger.info(f"Historical data of {ticker} is saved at {file_path}.")

            # print a sneak peek
            print("\n Here's a sneak peek into the data.\n")
            print("____SNEAK PEEK (For last 3 Days)____\n")
            print(df.tail(3))


if __name__ == "__main__":
    fetcher = HistoricalDataFetcher()

    fetcher.run("AAPL")
