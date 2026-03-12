import os
import json
import time
import datetime
import redis
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel

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
# Market Time Check
# -------------------------------
def is_market_open():
    """
    Checks if the market is currently open (9:15 AM to 3:35 PM).
    """
    now = datetime.datetime.now().time()
    market_open = datetime.time(9, 15)
    market_close = datetime.time(15, 35)
    return market_open <= now <= market_close

# Redis Keys
QUEUE_5 = "NIFTY_CANDLE_QUEUE_5"
QUEUE_20 = "NIFTY_CANDLE_QUEUE_20"
DAY_VAR = "NIFTY_DAY_VAR"

def update_nifty_candle_queues(redis_client, candle):
    """
    Maintains two Redis queues: latest 5 and latest 20 candles.
    """
    candle_json = json.dumps(candle)
    
    # Update 5-candle queue
    redis_client.lpush(QUEUE_5, candle_json)
    redis_client.ltrim(QUEUE_5, 0, 4)
    
    # Update 20-candle queue
    redis_client.lpush(QUEUE_20, candle_json)
    redis_client.ltrim(QUEUE_20, 0, 19)

def calculate_nifty_ema(redis_client):
    """
    Calculates EMA5 and EMA20 from the Redis queues.
    EMA = (Close - Previous EMA) * multiplier + Previous EMA
    multiplier = 2 / (period + 1)
    """
    
    # Fetch existing variables to get previous EMA values
    day_vars = redis_client.hgetall(DAY_VAR)
    prev_ema5 = float(day_vars.get("EMA5", 0))
    prev_ema20 = float(day_vars.get("EMA20", 0))
    
    # Get the latest candle from Queue 5 (which is the newest)
    latest_candle_raw = redis_client.lindex(QUEUE_5, 0)
    if not latest_candle_raw:
        return prev_ema5, prev_ema20
    
    latest_close = json.loads(latest_candle_raw)["close"]
    
    # EMA 5
    if prev_ema5 == 0:
        # First time, use SMA of the available queue or just the close
        ema5 = latest_close
    else:
        multiplier5 = 2 / (5 + 1)
        ema5 = (latest_close - prev_ema5) * multiplier5 + prev_ema5
        
    # EMA 20
    if prev_ema20 == 0:
        ema20 = latest_close
    else:
        multiplier20 = 2 / (20 + 1)
        ema20 = (latest_close - prev_ema20) * multiplier20 + prev_ema20
        
    return round(ema5, 2), round(ema20, 2)

def get_yesterday_close(redis_client, fyers):
    """
    Fetches the official previous day's final close from Fyers Quotes API.
    Discrepancies occur with candle data due to NSE's weighted average closing calculation.
    """
    stored_vars = redis_client.hgetall(DAY_VAR)
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # If yesterday_close is already set for TODAY, just return it
    if "yesterday_close" in stored_vars and stored_vars.get("date") == today_str:
        return float(stored_vars["yesterday_close"])

    print("Fetching official yesterday's close from Fyers Quotes API...")
    try:
        data = {"symbols": "NSE:NIFTY50-INDEX"}
        response = fyers.quotes(data)
        
        if "d" in response and len(response["d"]) > 0:
            # 'prev_close' in quotes API is the official NSE adjusted closing price
            yest_close = response["d"][0]["v"].get("prev_close")
            if yest_close:
                redis_client.hset(DAY_VAR, "yesterday_close", yest_close)
                # Also ensure date is set if we're initializing
                if "date" not in stored_vars:
                    redis_client.hset(DAY_VAR, "date", today_str)
                print(f"Set official yesterday_close to {yest_close}")
                return float(yest_close)
    except Exception as e:
        print(f"Error fetching official yesterday close via quotes: {e}")
    
    # Fallback to history if quotes API fails or doesn't have it (rare)
    print("Falling back to history for yesterday close...")
    end_date = today_str
    start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    
    data_hist = {
        "symbol": "NSE:NIFTY50-INDEX",
        "resolution": "5",
        "date_format": "1",
        "range_from": start_date,
        "range_to": end_date,
        "cont_flag": "1"
    }
    
    try:
        response_hist = fyers.history(data_hist)
        if "candles" in response_hist and response_hist["candles"]:
            candles = response_hist["candles"]
            for i in range(len(candles) - 1, -1, -1):
                c_time = datetime.datetime.fromtimestamp(candles[i][0])
                if c_time.strftime("%Y-%m-%d") < today_str:
                    yest_close = candles[i][4]
                    redis_client.hset(DAY_VAR, "yesterday_close", yest_close)
                    print(f"Set yesterday_close to {yest_close} (from history fallback)")
                    return float(yest_close)
    except Exception as e:
        print(f"History fallback error: {e}")
        
    return 0

def initialize_nifty_analytics(redis_client, fyers):
    """
    Seeds Redis queues and daily variables from historical data at startup.
    This fixes issues with late starts and restarts.
    """
    print("Initialising NIFTY Analytics from history...")
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # 1. Fetch Official Yesterday Close
    yest_close = get_yesterday_close(redis_client, fyers)
    
    # 2. Fetch today's historical candles to seed day_open, high, low, and queues
    data = {
        "symbol": "NSE:NIFTY50-INDEX",
        "resolution": "5",
        "date_format": "1",
        "range_from": (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
        "range_to": today_str,
        "cont_flag": "1"
    }
    
    try:
        response = fyers.history(data)
        if "candles" not in response or not response["candles"]:
            print("No historical candles found for initialization.")
            return

        candles = response["candles"]
        today_candles = []
        
        # Filter candles for today
        for c in candles:
            if datetime.datetime.fromtimestamp(c[0]).strftime("%Y-%m-%d") == today_str:
                today_candles.append(c)

        if not today_candles:
            print("No candles for today found in history yet.")
            # Still seed the queues with the last 20 candles from whatever is available
            seed_candles = candles[-20:]
        else:
            # Seed statistics from today's candles
            day_open = today_candles[0][1] # First candle's open of the day
            day_high = max(c[2] for c in today_candles)
            day_low = min(c[3] for c in today_candles)
            latest_close = today_candles[-1][4]
            
            # Update Redis NIFTY_DAY_VAR
            updates = {
                "day_open": day_open,
                "day_high": day_high,
                "day_low": day_low,
                "close": latest_close,
                "date": today_str
            }
            redis_client.hmset(DAY_VAR, updates)
            print(f"Seeded today's stats: Open={day_open}, High={day_high}, Low={day_low}")
            
            # Use last 20 candles for queue seeding (might include some from yesterday if today just started)
            seed_candles = candles[-20:]

        # 3. Seed Queues and EMAs
        # Clear existing queues to avoid duplication on restart
        redis_client.delete(QUEUE_5)
        redis_client.delete(QUEUE_20)
        
        # We also need to seed the EMAs sequentially from these candles
        # Starting with the oldest available as the base
        ema5 = 0
        ema20 = 0
        
        for i, c_raw in enumerate(seed_candles):
            candle = {
                "timestamp": c_raw[0],
                "open": c_raw[1],
                "high": c_raw[2],
                "low": c_raw[3],
                "close": c_raw[4],
                "volume": c_raw[5]
            }
            
            # Update queues
            update_nifty_candle_queues(redis_client, candle)
            
            # Calculate rolling EMA
            alpha5 = 2 / (5 + 1)
            alpha20 = 2 / (20 + 1)
            
            if i == 0:
                ema5 = candle["close"]
                ema20 = candle["close"]
            else:
                ema5 = (candle["close"] - ema5) * alpha5 + ema5
                ema20 = (candle["close"] - ema20) * alpha20 + ema20

        # Store final EMAs
        redis_client.hmset(DAY_VAR, {
            "EMA5": round(ema5, 2),
            "EMA20": round(ema20, 2)
        })
        print(f"Queues and EMAs seeded. EMA5={round(ema5, 2)}, EMA20={round(ema20, 2)}")

    except Exception as e:
        print(f"Error during initialization: {e}")

def update_nifty_day_var(redis_client, candle):
    """
    Updates daily NIFTY variables in Redis.
    Handles daily reset and updates high/low/close/EMAs.
    """
    candle_time = datetime.datetime.fromtimestamp(candle['timestamp'])
    candle_date_str = candle_time.strftime("%Y-%m-%d")
    candle_time_str = candle_time.strftime("%H:%M")
    
    stored_vars = redis_client.hgetall(DAY_VAR)
    last_date = stored_vars.get("date")
    
    updates = {}
    
    # Start of a new day reset logic
    if last_date != candle_date_str:
        print(f"New day detected: {candle_date_str}. Resetting stats...")
        # Carry over previous day's close as yesterday_close
        if last_date:
             # Use the actual 'close' from Redis which was the final close of last session
             prev_close = stored_vars.get("close")
             if prev_close:
                 updates["yesterday_close"] = prev_close
        
        # Reset daily stats
        updates["day_open"] = candle["open"]
        updates["day_high"] = candle["high"]
        updates["day_low"] = candle["low"]
        updates["date"] = candle_date_str
    else:
        # Update high and low
        current_high = float(stored_vars.get("day_high", 0))
        current_low = float(stored_vars.get("day_low", 9999999))
        
        if candle["high"] > current_high:
            updates["day_high"] = candle["high"]
        if candle["low"] < current_low:
            updates["day_low"] = candle["low"]
            
        if "day_open" not in stored_vars and candle_time_str == "09:15":
            updates["day_open"] = candle["open"]

    # Always update current close
    updates["close"] = candle["close"]
    
    # Calculate and update EMAs
    ema5, ema20 = calculate_nifty_ema(redis_client)
    updates["EMA5"] = ema5
    updates["EMA20"] = ema20
    
    if updates:
        redis_client.hmset(DAY_VAR, updates)
    
    return updates


def fetch_nifty_index_candle():
    """
    Fetches the latest 5-minute candle for NIFTY 50-INDEX.
    """
    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        log_path=""
    )
    
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    data = {
        "symbol": "NSE:NIFTY50-INDEX",
        "resolution": "5",
        "date_format": "1",
        "range_from": today,
        "range_to": today,
        "cont_flag": "1"
    }
    
    try:
        response = fyers.history(data)
        if "candles" in response and response["candles"]:
            raw_candle = response["candles"][-1]
            return {
                "timestamp": raw_candle[0],
                "open": raw_candle[1],
                "high": raw_candle[2],
                "low": raw_candle[3],
                "close": raw_candle[4],
                "volume": raw_candle[5]
            }
    except Exception as e:
        print(f"Error fetching NIFTY candle: {e}")
    
    return None


def main():
    print("NIFTY Analytics System Started")
    
    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        log_path=""
    )

    # Robust Initialization from history
    initialize_nifty_analytics(redis_client, fyers)
    
    while True:
        now = datetime.datetime.now()
        
        # Only run during market hours (plus a small buffer)
        if is_market_open():
            # Align with 5-minute interval
            if now.minute % 5 == 0 and now.second < 10:
                print(f"[{now}] Processing NIFTY candle...")
                candle = fetch_nifty_index_candle()
                
                if candle:
                    # Update queues and daily variables
                    update_nifty_candle_queues(redis_client, candle)
                    updates = update_nifty_day_var(redis_client, candle)
                    print(f"Updated NIFTY Analytics: {updates}")
                else:
                    print("No candle data available.")
                    
                # Sleep to avoid multiple processing in same minute
                time.sleep(60)
        else:
            # When market is closed, we still want to ensure yesterday_close is updated for next day
            # or if we are just starting up.
            if now.hour == 9 and now.minute == 0 and now.second < 10:
                initialize_nifty_analytics(redis_client, fyers)
                time.sleep(60)
            
            # Print heart beat less frequently when market is closed
            if now.minute % 30 == 0 and now.second < 5:
                print(f"[{now}] Market is closed. Waiting...")
                time.sleep(10)
        
        time.sleep(1)


if __name__ == "__main__":
    main()