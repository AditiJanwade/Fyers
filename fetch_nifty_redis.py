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
        
        # split groups
        top15 = market_caps[:15]
        rest35 = market_caps[15:]

        # total market caps
        total_t15_cap = sum(stock[1] for stock in top15)
        total_r35_cap = sum(stock[1] for stock in rest35)

        # create tuples with weights
        T15 = tuple(
            (stock, market_cap, round((market_cap / total_t15_cap) * 100, 4))
            for stock, market_cap in top15
        )

        R35 = tuple(
            (stock, market_cap, round((market_cap / total_r35_cap) * 100, 4))
            for stock, market_cap in rest35
        )

        redis_client.set("T15", json.dumps(T15))
        redis_client.set("R35", json.dumps(R35))

        # mark as done for today
        redis_client.set(redis_flag, "done")

        print("Market cap calculated.")

    except Exception as e:
        print(f"Market Cap Error: {e}")

# -------------------------------
# Calculate R35 Participation & Acceleration
# -------------------------------
def calculate_r35_participation_and_acceleration(delta=0.01):

    try:
        r35 = json.loads(redis_client.get("R35"))

        # load previous metrics
        metrics_raw = redis_client.get("market_metrics")
        prev_parti_intra_low = 0
        prev_parti_intra_high = 0

        if metrics_raw:
            prev_metrics = json.loads(metrics_raw)
            prev_parti_intra_low = prev_metrics.get("parti_intra_low", 0)
            prev_parti_intra_high = prev_metrics.get("parti_intra_high", 0)

        parti_intra_low = 0
        parti_intra_high = 0

        for stock, mc, weight in r35:

            stock_raw = redis_client.get(stock)
            if not stock_raw:
                continue

            data = json.loads(stock_raw)

            LTP = data["close"]
            day_low = data["day_low"]
            day_high = data["day_high"]

            low_threshold = day_low + (delta * day_low)
            high_threshold = day_high - (delta * day_high)

            if LTP > low_threshold:
                parti_intra_low += 1

            if LTP < high_threshold:
                parti_intra_high += 1

        # acceleration
        acc_low = parti_intra_low - prev_parti_intra_low
        acc_high = parti_intra_high - prev_parti_intra_high

        return parti_intra_low, parti_intra_high, acc_low, acc_high

    except Exception as e:
        print("Participation Error:", e)
        return 0,0,0,0

# -------------------------------
# Calculate Group Strength
# -------------------------------
def calculate_group_strength():

    try:
        t15 = json.loads(redis_client.get("T15"))
        r35 = json.loads(redis_client.get("R35"))

        # -----------------------
        # Calculate total market cap
        # -----------------------
        total_mc = 0

        for stock, mc, weight in t15:
            total_mc += mc

        for stock, mc, weight in r35:
            total_mc += mc

        # -----------------------
        # T15 calculations
        # -----------------------
        t15_net_weight = 0
        t15_total_mc = 0

        for stock, mc, weight in t15:

            t15_total_mc += mc

            stock_raw = redis_client.get(stock)
            if not stock_raw:
                continue

            stock_data = json.loads(stock_raw)

            close = stock_data["close"]
            prev_close = stock_data["yesterday_close"]

            if not prev_close:
                continue

            contribution = weight * ((close - prev_close) / close)

            t15_net_weight += contribution

        t15_mc_percent = t15_total_mc / total_mc

        t15_strength = (t15_net_weight / t15_mc_percent) 

        # -----------------------
        # R35 calculations
        # -----------------------
        r35_net_weight = 0
        r35_total_mc = 0

        for stock, mc, weight in r35:

            r35_total_mc += mc

            stock_raw = redis_client.get(stock)
            if not stock_raw:
                continue

            stock_data = json.loads(stock_raw)

            close = stock_data["close"]
            prev_close = stock_data["yesterday_close"]

            if not prev_close:
                continue

            contribution = weight * ((close - prev_close) / close)

            r35_net_weight += contribution
        
        r35_mc_percent = r35_total_mc / total_mc

        r35_strength = (r35_net_weight / r35_mc_percent)

          # ---- Momentum ----
        momentum = t15_strength + r35_strength

        # ---- Cap momentum ----
        cap_momentum = max(-30, min(30, momentum))

        # ---- Normalize ----
        normalize_mon = ((30 + cap_momentum) / 60) * 100
        parti_intra_low, parti_intra_high, acc_low, acc_high = calculate_r35_participation_and_acceleration()

        metrics = {
            "t15_strength": round(t15_strength,4),
            "r35_strength": round(r35_strength,4),
            "momentum": round(momentum,4),
            "cap_momentum": round(cap_momentum,4),
            "normalize_momentum": round(normalize_mon,2),
            "parti_intra_low": parti_intra_low,
            "parti_intra_high": parti_intra_high,
            "acc_low": acc_low,
            "acc_high": acc_high
        }

        # -----------------------
        # Store in Redis
        # -----------------------

        redis_client.set("market_metrics", json.dumps(metrics))

        print("t15 mc percent:", round(t15_mc_percent, 4))
        print("r35 mc percent:", round(r35_mc_percent, 4))
        print("t15 net weight:", round(t15_net_weight, 4))
        print("r35 net weight:", round(r35_net_weight, 4))

        print(metrics)

    except Exception as e:
        print("Strength Calculation Error:", e)


# -------------------------------
# Fetch Day Levels
# -------------------------------
def fetch_day_levels(symbol, fyers):

    try:
        data = {"symbols": symbol}

        response = fyers.quotes(data)

        # print("QUOTE RESPONSE:", symbol, response)

        if "d" not in response or len(response["d"]) == 0:
            return None

        v = response["d"][0]["v"]

        return {
            "day_high": v.get("high_price"),
            "day_low": v.get("low_price"),
            "yesterday_close": v.get("prev_close_price")
        }

    except Exception as e:
        print(f"Quote Error ({symbol}): {e}")
        return None
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

                if last_timestamp is None or market_timestamp >= last_timestamp:

                    if existing_raw:
                        existing = json.loads(existing_raw)

                        day_high = max(existing.get("day_high", candle["high"]), candle["high"])
                        day_low = min(existing.get("day_low", candle["low"]), candle["low"])

                        candle["day_high"] = day_high
                        candle["day_low"] = day_low
                        # Fix yesterday_close
                        if existing.get("yesterday_close") is None:

                            levels = fetch_day_levels(symbol, fyers)

                            if levels:
                                candle["yesterday_close"] = levels["yesterday_close"]
                            else:
                                candle["yesterday_close"] = None

                        else:
                            candle["yesterday_close"] = existing["yesterday_close"]
                    else:
                        # first run → fetch day values
                        levels = fetch_day_levels(symbol, fyers)

                        if levels:
                            candle["day_high"] = levels["day_high"]
                            candle["day_low"] = levels["day_low"]
                            candle["yesterday_close"] = levels["yesterday_close"]
                        else:
                            candle["day_high"] = candle["high"]
                            candle["day_low"] = candle["low"]
                            candle["yesterday_close"] = None

                    redis_client.set(redis_key, json.dumps(candle))

                    print(f"Updated latest candle for {symbol}: {datetime.datetime.fromtimestamp(market_timestamp)}")
               
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
    
    # Calculate group strength
    calculate_group_strength()

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