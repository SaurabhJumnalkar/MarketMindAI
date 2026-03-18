import joblib
import streamlit as st
from huggingface_hub import hf_hub_download
from dotenv import load_dotenv

# import sys
import os
import pandas as pd

# load local .venv if running on local
load_dotenv()

# Setup of page
st.set_page_config(page_title="MarketMindAI", page_icon="🧠", layout="centered")

# Hugging face
HF_USERNAME = "saurabhSJ"
REPO_NAME = f"{HF_USERNAME}/MarketMind-Data"


def get_cloud_data(ticker: str):
    token = os.environ.get("HF_TOKEN")

    try:
        model_path = hf_hub_download(
            repo_id=REPO_NAME,
            filename=f"models/{ticker}_model.joblib",
            repo_type="dataset",
            token=token,
        )
        data_path = hf_hub_download(
            repo_id=REPO_NAME,
            filename=f"data/{ticker}_master.csv",
            repo_type="dataset",
            token=token,
        )

        model = joblib.load(model_path)
        df = pd.read_csv(data_path)

        return model, df

    except Exception as e:
        print(f"Error While downloading the data & model: {e}")
        return None, None


def main():
    # Header
    st.title("MarketMindAI 🧠")
    st.markdown("**AI Powered Stock Risk Analyzer**")
    st.markdown("---")

    # User input and button
    ticker = st.text_input("Enter stock Ticker (eg. AAPL, TSLA)").upper()

    if st.button("Analyze Risk"):
        with st.spinner(f"Analyzing the risk for {ticker}..."):
            # Predicting the risk
            # load the data
            model, df = get_cloud_data(ticker)

            if model is None or df is None:
                st.error(f"We don't have trained model for {ticker}.")
                st.warning(f"You need to run pipeline for getting model for {ticker}.")
                return

            today_data = df.iloc[-1:]
            Date = today_data["Date"].values[0]
            Close = today_data["Close"].values[0]
            Daily_sentiment_score = today_data["Daily_sentiment_score"].values[0]

            # prep the data
            cols_to_drop = [
                "Date",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
                "Dividends",
                "Stock Splits",
                "Target_direction",
            ]
            X_today = today_data.drop(columns=cols_to_drop, errors="ignore")

            prediction = model.predict(X_today)[0]
            direction = "UP" if prediction == 1 else "DOWN"

            # The UI dashboard
            st.success(f"Analysis is Done.last updated: {Date}")

            # 3 columns for our data
            col1, col2, col3 = st.columns(3)

            col1.metric("Closing Price", f"${Close:.2f}")  # noqa E231
            col2.metric("News Sentiment", f"{Daily_sentiment_score:.2f}")  # noqa E231
            col3.metric("AI Prediction", direction)  # noqa E231

            st.markdown("---")
            st.markdown("**Recent market Trends(last 30 Days)**")

            # Chart from stremlit
            recent_data = df.tail(30).set_index("Date")
            st.line_chart(
                recent_data["Close"],
                height="stretch",
            )


if __name__ == "__main__":
    main()
