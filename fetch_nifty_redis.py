import os
import json
import time
import datetime
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
import redis
import schedule

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# -------------------------------
# Redis Connection
# -------------------------------
try:
    redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True
)

    redis_client.ping()
    print("Connected to Redis successfully.")
except Exception as e:
    print(f"Redis connection failed: {e}")
    exit()

# -------------------------------
# NIFTY 50 Symbols
# -------------------------------
NIFTY_50_SYMBOLS = [
    "NSE:SUNPHARMA-EQ",
"NSE:JIOFIN-EQ",
"NSE:COALINDIA-EQ",
"NSE:NTPC-EQ",
"NSE:ONGC-EQ",
"NSE:DRREDDY-EQ",
"NSE:HINDALCO-EQ",
"NSE:WIPRO-EQ",
"NSE:POWERGRID-EQ",
"NSE:ITC-EQ",
"NSE:TECHM-EQ",
"NSE:TATASTEEL-EQ",
"NSE:CIPLA-EQ",
"NSE:ADANIENT-EQ",
"NSE:INDIGO-EQ",
"NSE:GRASIM-EQ",
"NSE:MAXHEALTH-EQ",
"NSE:NESTLEIND-EQ",
"NSE:ADANIPORTS-EQ",
"NSE:HDFCLIFE-EQ",
"NSE:APOLLOHOSP-EQ",
"NSE:HCLTECH-EQ",
"NSE:SBILIFE-EQ",
"NSE:TATACONSUM-EQ",
"NSE:TMPV-EQ",
"NSE:ASIANPAINT-EQ",
"NSE:TRENT-EQ",
"NSE:ETERNAL-EQ",
"NSE:ULTRACEMCO-EQ",
"NSE:HINDUNILVR-EQ",
"NSE:TITAN-EQ",
"NSE:BAJAJ-AUTO-EQ",
"NSE:BEL-EQ",
"NSE:JSWSTEEL-EQ",
"NSE:SHRIRAMFIN-EQ",
"NSE:EICHERMOT-EQ",
"NSE:BAJAJFINSV-EQ",
"NSE:LT-EQ",
"NSE:MARUTI-EQ",
"NSE:TCS-EQ",
"NSE:INFY-EQ",
"NSE:KOTAKBANK-EQ",
"NSE:SBIN-EQ",
"NSE:M&M-EQ",
"NSE:BHARTIARTL-EQ",
"NSE:BAJFINANCE-EQ",
"NSE:ICICIBANK-EQ",
"NSE:RELIANCE-EQ",
"NSE:AXISBANK-EQ",
"NSE:HDFCBANK-EQ"
]

# -------------------------------
# Market Time Check
# -------------------------------
def is_market_open():

    now = datetime.datetime.now().time()

    market_open = datetime.time(9, 15)
    market_close = datetime.time(15, 35)

    return market_open <= now <= market_close

# -------------------------------
# Calculate Market Caps Once
# -------------------------------
def calculate_market_caps_once():

    today = datetime.date.today().isoformat()
    redis_flag = f"market_cap_done:{today}"

    # If already calculated today, skip
    if redis_client.exists(redis_flag):
        return

    try:
        with open("floating_shares.json", "r") as f:
            floating_shares = json.load(f)

        market_caps = []

        for stock, shares in floating_shares.items():

            data = redis_client.get(stock)

            if not data:
                continue

            candle = json.loads(data)

            close_price = candle["close"]

            market_cap = close_price * shares

            market_caps.append((stock, market_cap))

        if not market_caps:
            return

        market_caps.sort(key=lambda x: x[1], reverse=True)

        T15 = tuple(market_caps[:15])
        R35 = tuple(market_caps[15:])

        redis_client.set("T15", json.dumps(T15))
        redis_client.set("R35", json.dumps(R35))

        # mark as done for today
        redis_client.set(redis_flag, "done")

        print("Market cap calculated.")

    except Exception as e:
        print(f"Market Cap Error: {e}")

# -------------------------------
# Fetch Data from Fyers
# -------------------------------
def fetch_and_store_data(force=False):
    """
    Fetches 5-minute candle data and stores it with the ORIGINAL market timestamp.
    - Uses Redis list storage to keep daily history.
    - Prevents duplicates by checking the last stored timestamp.
    """

    print(f"\n[{datetime.datetime.now()}] Fetching latest 5-minute candles...")

    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        log_path=""
    )

    # Use a 3-day range to ensure we get data even at midnight or over weekends
    # but we will only STORE the single most recent candle found.
    now = datetime.datetime.now()
    end_date = now.strftime("%Y-%m-%d")
    start_date = (now - datetime.timedelta(days=3)).strftime("%Y-%m-%d")

    for symbol in NIFTY_50_SYMBOLS:
        data = {
            "symbol": symbol,
            "resolution": "5",
            "date_format": "1",
            "range_from": start_date,
            "range_to": end_date,
            "cont_flag": "1"
        }

        try:
            response = fyers.history(data)

            if "candles" not in response or not response["candles"]:
                print(f"No data for {symbol}")
                continue

            # Get the latest market candle
            raw_candle = response["candles"][-1]
            market_timestamp = raw_candle[0]

            candle = {
                "timestamp": market_timestamp,
                "open": raw_candle[1],
                "high": raw_candle[2],
                "low": raw_candle[3],
                "close": raw_candle[4],
                "volume": raw_candle[5]
            }

            # IMPORTANT: Extract stock name only (e.g., SUNPHARMA)
            # This ensures keys match floating_shares.json and market_cap.py
            redis_key = symbol.split(":")[1].replace("-EQ", "")

            # -------------------------------
            # Duplicate Check & Single-Candle Storage
            # -------------------------------
            try:
                # Handle potential WRONGTYPE (if it was a list previously)
                key_type = redis_client.type(redis_key)
                if key_type != "string" and key_type != "none":
                    print(f"Fixing type for {symbol} ({key_type} -> string)")
                    redis_client.delete(redis_key)

                existing_raw = redis_client.get(redis_key)
                last_timestamp = None
                if existing_raw:
                    last_timestamp = json.loads(existing_raw)["timestamp"]

                if last_timestamp is None or market_timestamp > last_timestamp:
                    # Overwrite with only the latest candle
                    redis_client.set(redis_key, json.dumps(candle))
                    print(f"Updated latest candle for {symbol}: {datetime.datetime.fromtimestamp(market_timestamp)}")
                else:
                    # No new candle available
                    pass

            except Exception as e:
                if "WRONGTYPE" in str(e):
                    print(f"Auto-fixing WRONGTYPE for {symbol}")
                    redis_client.delete(redis_key)
                    redis_client.set(redis_key, json.dumps(candle))
                else:
                    print(f"Redis Error ({symbol}): {e}")

        except Exception as e:
            print(f" API Error ({symbol}): {e}")

        # prevent API rate limit
        time.sleep(0.1)

    # Calculate market caps once
    calculate_market_caps_once()

    print(f"[{datetime.datetime.now()}] Fetch Cycle Completed")


# -------------------------------
# Scheduler aligned to candle time
# -------------------------------
def schedule_fetch():
    """
    Schedules the fetch to run every 5 minutes.
    """
    for minute in range(0, 60, 5):
        schedule.every().hour.at(f":{minute:02d}").do(fetch_and_store_data)


# -------------------------------
# Main
# -------------------------------
def main():
    print("Continuous Market Data Collector Started")
    print("Connected to Redis")

    # Run once immediately
    fetch_and_store_data(force=True)

    # Schedule for continuous updates (no market hours guard)
    schedule_fetch()

    print("Scheduler running every 5 minutes (Running Time updates)...")

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            print("\n Stopping collector...")
            break
        except Exception as e:
            print(f" Main Loop Error: {e}")
            time.sleep(5)


# -------------------------------
# Run Script
# -------------------------------
if __name__ == "__main__":
    main()