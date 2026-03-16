import joblib
import sys
import os
import pandas as pd


class MarketMindChatBot:
    def __init__(self):
        print("\n==================================================")
        print("\n==========Welcome To MARKET MIND AI===============\n")

    def get_predictions(self, ticker: str):
        model_path = f"models/{ticker}_model.joblib"
        data_path = f"data/{ticker}_master.csv"

        if not os.path.exists(model_path) or not os.path.exists(data_path):
            print(f"\n Trained Model for {ticker} is not present.")
            print(f"\nPlease run the pipeline to get trained model for {ticker}")
            return

        model = joblib.load(model_path)
        df = pd.read_csv(data_path)

        # Get today's data
        # Because based upon today's price and news sentiment our model give output

        todays_data = pd.DataFrame(df.iloc[-1:])

        date = todays_data["Date"].values[0]
        close = todays_data["Close"].values[0]
        sentiment = todays_data["Daily_sentiment_score"].values[0]

        cols_to_drop = [
            "Date",
            "Close",
            "Target_direction",
            "Open",
            "High",
            "Low",
            "Volume",
            "Dividends",
            "Stock Splits",
        ]
        X_today = todays_data.drop(columns=cols_to_drop, errors="ignore")

        prediction = model.predict(X_today)[0]

        predicted_direction = "UP" if prediction == 1 else "DOWN"

        print(f"\n ========Risk Report for {ticker}=========")
        print(f"\n Last Data Date: {date}")
        print(f"\n Last Close Price: {close}")
        print(f"\n News Sentiment: {sentiment:.2f} (-1.0 to 1.0)")  # noqa E231
        print("-" * 20)

        print(
            f"AI PREDICTION: {ticker} Stock will go tomorrow {predicted_direction}.\n"
        )


if __name__ == "__main__":
    marketMindChatBot = MarketMindChatBot()

    while True:
        user_input = input("\nEnter stock ticker or type 'exit\n").strip().upper()

        if user_input == "EXIT":
            print("\n SHUTTING DOWN MARKETMIND AI BOT\n")
            sys.exit(0)

        if user_input == "":
            continue

        marketMindChatBot.get_predictions(user_input)
