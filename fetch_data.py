import os
import datetime
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
import pandas as pd

load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")


def fetch_candle_data(symbol="NSE:NIFTY50-INDEX"):     

    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        log_path=""
    )

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    data = {
        "symbol": symbol,
        "resolution": "5",       # 5-minute candles
        "date_format": "1",
        "range_from": today,
        "range_to": today,
        "cont_flag": "1"
    }

    response = fyers.history(data)

    candles = response["candles"]

    df = pd.DataFrame(candles, columns=[
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ])

    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Kolkata")

    # df.to_csv("candle_data.csv", index=False)

    return df


if __name__ == "__main__":

    df = fetch_candle_data()
    print(df)