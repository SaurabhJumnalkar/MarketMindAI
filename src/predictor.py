import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import logging
import sys
import os
import joblib


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class StockPredictor:
    def __init__(self, ticker: str):
        self.ticker = ticker
        self.data_path = f"data/{ticker}_master.csv"
        os.makedirs("models", exist_ok=True)

    def load_and_prep_data(self):
        if not os.path.exists(self.data_path):
            logger.error(f"Data not found at {self.data_path}.")
            sys.exit(1)

        df = pd.read_csv(self.data_path)

        X = df.drop(
            columns=[
                "Open",
                "High",
                "Low",
                "Volume",
                "Date",
                "Close",
                "Target_direction",
            ]
        )
        y = df["Target_direction"]

        return X, y

    def run(self):
        logger.info(f"______Training AI for {self.ticker}_______")

        X, y = self.load_and_prep_data()

        # Shuffle False because we need sorted data acc to Date
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, shuffle=False
        )
        logger.info(
            f"Training on {len(X_train)} days of history and testing on {len(X_test)} unseen days..."
        )

        # Initialize the AI brain
        model = xgb.XGBClassifier(
            n_estimators=100, max_depth=3, learning_rate=0.1, random_state=42
        )

        # study the data
        model.fit(X_train, y_train)

        # Take final exam
        predictions = model.predict(X_test)

        # Grade the Exam
        accuracy = accuracy_score(y_test, predictions)
        logger.info(
            f"Model Accuracy on Unseen Data: {accuracy * 100:.2f}% ."  # noqa E231
        )

        # Ask the AI how it made the decisions
        importance = model.feature_importances_
        features = X.columns
        print("\n ____What the AI Cares About____\n")
        for feature, imp in zip(features, importance):
            print(f"{feature}: {imp * 100:.1f}% ")  # noqa E231

        model_path = f"models/{self.ticker}_model.joblib"
        joblib.dump(model, model_path)
        logger.info(f"Successfully Saved trained Model at {model_path}")


if __name__ == "__main__":
    predictor = StockPredictor("AAPL")
    predictor.run()
