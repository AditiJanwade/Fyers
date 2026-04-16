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

fyers = fyersModel.FyersModel(client_id=CLIENT_ID, token=ACCESS_TOKEN,is_async=False, log_path="")
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
CONTEXT_VAR = "NIFTY_CONTEXT_VAR"


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
            redis_client.hset(DAY_VAR, mapping=updates)
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
        redis_client.hset(DAY_VAR, mapping={
            "EMA5": round(ema5, 2),
            "EMA20": round(ema20, 2)
        })
        print(f"Queues and EMAs seeded. EMA5={round(ema5, 2)}, EMA20={round(ema20, 2)}")

    except Exception as e:
        print(f"Error during initialization: {e}")



def calculate_nifty_context(redis_client):
    vars = redis_client.hgetall(DAY_VAR)

    if not vars:
        return {}

    try:
        ltp = float(vars.get("close", 0))
        day_high = float(vars.get("day_high", 0))
        day_low = float(vars.get("day_low", 0))
        ema5 = float(vars.get("EMA5", 0))
        ema20 = float(vars.get("EMA20", 0))
        yclose = float(vars.get("yesterday_close", 0))
    except:
        return {}

    updates = {}

    # A) Position
    pos = (ltp - day_low) / (day_high - day_low) if day_high != day_low else 0
    updates["pos"] = round(pos, 4)
    updates["pos_pct"] = round(pos * 100, 2)

    # B) Slope
    prev = redis_client.lindex(QUEUE_5, 1)
    if prev:
        prev_close = json.loads(prev)["close"]
        updates["slope"] = round(ltp - prev_close, 2)
    else:
        updates["slope"] = 0

    # C) Trend
    updates["trend_up"] = 1 if ema5 > ema20 else 0
    updates["trend_down"] = 1 if ema5 < ema20 else 0

    # D) Recovery
    if day_low < yclose and (yclose - day_low) != 0:
        rec = (ltp - day_low) / (yclose - day_low)
    else:
        rec = 0

    updates["rec_ratio"] = round(rec, 4)
    updates["rec_ratio_pct"] = round(rec * 100, 2)

    # E) Top Recently
    candles = redis_client.lrange(QUEUE_5, 0, 5)
    pos_list = []

    for c in candles:
        c = json.loads(c)
        if day_high != day_low:
            pos_list.append((c["close"] - day_low) / (day_high - day_low))

    updates["top_recently"] = 1 if pos_list and max(pos_list) > 0.75 else 0

    # SAVE
    redis_client.hset(CONTEXT_VAR, mapping=updates)

    return updates



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
        redis_client.hset(DAY_VAR, mapping=updates)
    
    return updates

# -------------------------------
# OI LOCK + RESET LOGIC
# -------------------------------
LOCK_KEY = "NIFTY_OI_LOCK"

def get_or_create_lock(redis_client, options, nifty_ltp):
    lock_data = redis_client.get(LOCK_KEY)

    # -----------------------------
    # GET DAY OPEN
    # -----------------------------
    day_vars = redis_client.hgetall(DAY_VAR)
    day_open = float(day_vars.get("day_open", 0))

    if lock_data:
        lock = json.loads(lock_data)
        base_price = lock["base_price"]

        # RESET if 500 move
        if abs(nifty_ltp - base_price) >= 500:
            print("🔁 500 MOVE → RESETTING LOCK")
            redis_client.delete(LOCK_KEY)
        else:
            return lock

    # -----------------------------
    # CREATE NEW LOCK (USE OPEN 🔥)
    # -----------------------------
    base_price = day_open if day_open else nifty_ltp

    strikes = sorted(set(
        item["strike_price"]
        for item in options
        if item["strike_price"] != -1
    ))

    atm_index = min(range(len(strikes)), key=lambda i: abs(strikes[i] - base_price))

    selected_strikes = strikes[
        max(0, atm_index - 10): atm_index + 11
    ]

    lock = {
        "base_price": base_price,
        "strikes": selected_strikes
    }

    redis_client.set(LOCK_KEY, json.dumps(lock))

    print(f"🔒 Lock Created at OPEN {round(base_price)}")

    return lock


# -------------------------------
# OI RATIO CALCULATION
# -------------------------------
def calculate_oi_ratio(options, selected_strikes):
    ce_total = 0
    pe_total = 0

    ce_atm = 0
    pe_atm = 0

    atm_strike = selected_strikes[len(selected_strikes)//2]
    atm_range = [atm_strike - 50, atm_strike, atm_strike + 50]

    for item in options:
        strike = item["strike_price"]

        if strike in selected_strikes:
            if item["option_type"] == "CE":
                ce_total += item["oi"]
            elif item["option_type"] == "PE":
                pe_total += item["oi"]

        if strike in atm_range:
            if item["option_type"] == "CE":
                ce_atm += item["oi"]
            elif item["option_type"] == "PE":
                pe_atm += item["oi"]

    ce_ratio = ce_total / ce_atm if ce_atm else 0
    pe_ratio = pe_total / pe_atm if pe_atm else 0

    return {
        "ce_total": ce_total,
        "pe_total": pe_total,
        "ce_atm": ce_atm,
        "pe_atm": pe_atm,
        "ce_ratio": round(ce_ratio, 2),
        "pe_ratio": round(pe_ratio, 2)
    }
    
# -------------------------------
# BUILD OI FEATURE SNAPSHOT
# -------------------------------
def build_oi_feature_snapshot(redis_client):
    try:
        # -------------------------
        # MARKET CI
        # -------------------------
        metrics = json.loads(redis_client.get("market_metrics") or "{}")
        ci = metrics.get("ci_final", 0)

        # -------------------------
        # OI DATA
        # -------------------------
        oi_raw = redis_client.get("NIFTY_OI_20")
        if not oi_raw:
            return None

        oi_data = json.loads(oi_raw)

        pe_data = {int(k): v for k, v in oi_data.get("PE", {}).items()}
        ce_data = {int(k): v for k, v in oi_data.get("CE", {}).items()}
        strikes = oi_data.get("strikes", [])
        expiry = oi_data.get("expiry")

        if not strikes:
            return None

        
        day_vars = redis_client.hgetall("NIFTY_DAY_VAR")
        # -------------------------
        # SUPPORT / RESISTANCE (Directional)
        # -------------------------
        nifty_price = float(day_vars.get("close", 0))

        atm_index = min(
            range(len(strikes)),
            key=lambda i: abs(strikes[i] - nifty_price)
        )

        # CE → Resistance side (ATM + 1, +2)
        ce_strikes = strikes[atm_index : min(len(strikes), atm_index + 3)]

        # PE → Support side (ATM, -1, -2)
        pe_strikes = strikes[max(0, atm_index - 2) : atm_index + 1]

        total_pe = sum(pe_data.values())
        total_ce = sum(ce_data.values())

        near_pe = sum(pe_data.get(s, 0) for s in pe_strikes)
        near_ce = sum(ce_data.get(s, 0) for s in ce_strikes)

        support = (near_pe / total_pe * 100) if total_pe else 0
        resistance = (near_ce / total_ce * 100) if total_ce else 0

        # -------------------------
        # PCR & OI BIAS
        # -------------------------
        pcr = (total_pe / total_ce) if total_ce else 0
        oi_bias = support - resistance

        # -------------------------
        # NIFTY SPOT PRICE
        # -------------------------
        day_vars = redis_client.hgetall("NIFTY_DAY_VAR")
        nifty_price = float(day_vars.get("close", 0))

        # -------------------------
        # NIFTY FUTURE PRICE
        # -------------------------
        fut_price = 0

        try:
            now = datetime.datetime.now()

            year = str(now.year)[-2:]
            month = now.strftime("%b").upper()

            fut_symbol = f"NSE:NIFTY{year}{month}FUT"

            response = fyers.quotes({"symbols": fut_symbol})

            print("FUT SYMBOL:", fut_symbol)
            print("FUT RESPONSE:", response)

            if "d" in response and response["d"]:
                data = response["d"][0]["v"]

                fut_price = data.get("lp") or data.get("last_price") or 0

        except Exception as e:
            print("FUT Error:", e)  

        # -------------------------
        # FINAL FEATURE OBJECT
        # -------------------------
        return {
            "ci": round(ci, 2),
            "support": round(support, 2),
            "resistance": round(resistance, 2),
            "oi_bias": round(oi_bias, 2),
            "pcr": round(pcr, 2),
            "nifty_price": nifty_price,
            "nifty_fut_price": fut_price
        }

    except Exception as e:
        print("Feature Build Error:", e)
        return None

def store_oi_feature_snapshot(redis_client):
    try:
        features = build_oi_feature_snapshot(redis_client)


        if not features:
            return

        oi_bias = features.get("oi_bias", 0)

        # ALERT CONDITION
        if oi_bias >= 15:
            print(f"🚀 STRONG BULLISH BIAS at {datetime.datetime.now().strftime('%H:%M')} → {oi_bias}")

        elif oi_bias <= -15:
            print(f"🔻 STRONG BEARISH BIAS at {datetime.datetime.now().strftime('%H:%M')} → {oi_bias}")     

        # get latest candle timestamp
        latest_candle_raw = redis_client.lindex("NIFTY_CANDLE_QUEUE_5", 0)

        if not latest_candle_raw:
            return

        candle = json.loads(latest_candle_raw)

        candle_time = datetime.datetime.fromtimestamp(candle["timestamp"])

        date_key = candle_time.strftime("%Y-%m-%d")
        time_key = candle_time.strftime("%H:%M")

        redis_key = "OI_FEATURE_LIVE"

        existing = redis_client.hget(redis_key, date_key)

        if existing:
            day_data = json.loads(existing)
        else:
            day_data = {}

        day_data[time_key] = features

        redis_client.hset(redis_key, date_key, json.dumps(day_data))

        print(f"✅ OI Feature Saved → {time_key}")

    except Exception as e:
        print("OI Store Error:", e)
        
def run_nifty_oi_once(redis_client, fyers):
    data = {
        "symbol": "NSE:NIFTY50-INDEX",
        "strikecount": 50,
        "timestamp": ""
    }

    try:
        response = fyers.optionchain(data=data)

        if response["code"] != 200:
            print("OI fetch error")
            return

        redis_client.set("NIFTY_OI_RAW", json.dumps(response))

        raw = response["data"]
        options = raw["optionsChain"]

        # -----------------------------
        # 1. LATEST EXPIRY
        # -----------------------------
        expiry_list = raw.get("expiryData", [])
        latest_expiry = expiry_list[0]["date"]

        # -----------------------------
        # 2. GET LTP (ATM reference)
        # -----------------------------
        nifty_ltp = next(
            item["ltp"] for item in options if item["strike_price"] == -1
        )

       

        # 🔒 LOCK STRIKES
        lock = get_or_create_lock(redis_client, options, nifty_ltp)
        selected_strikes = lock["strikes"]

        # -----------------------------
        # 3. FILTER CE & PE
        # -----------------------------
        ce_data = {}
        pe_data = {}

        for item in options:
            strike = item["strike_price"]

            if strike in selected_strikes:

                if item["option_type"] == "CE":
                    ce_data[strike] = item["oi"]

                elif item["option_type"] == "PE":
                    pe_data[strike] = item["oi"]

        ratio_data = calculate_oi_ratio(options, selected_strikes)
        # -----------------------------
        # 4. STORE IN REDIS
        # -----------------------------
        final_data = {
            "expiry": latest_expiry,
            "ltp": nifty_ltp,
            "base_price": lock["base_price"],
            "strikes": selected_strikes,
            "CE": ce_data,
            "PE": pe_data,
            "analysis": ratio_data, 
            "timestamp": datetime.datetime.now().strftime("%H:%M:%S")
        }

        redis_client.set("NIFTY_OI_20", json.dumps(final_data))

        # -----------------------------
        # STORE OI HISTORY (APPEND)
        # -----------------------------
        try:
            # ✅ Get candle-based time (NOT system time)
            latest_candle_raw = redis_client.lindex("NIFTY_CANDLE_QUEUE_5", 0)

            if not latest_candle_raw:
                print("No candle for OI timestamp ❌")
                return

            candle = json.loads(latest_candle_raw)
            candle_time = datetime.datetime.fromtimestamp(candle["timestamp"])

            date_key = candle_time.strftime("%Y-%m-%d")
            time_key = candle_time.strftime("%H:%M")

            history_key = "NIFTY_OI_HISTORY"

            # get existing day data
            existing = redis_client.hget(history_key, date_key)

            if existing:
                day_data = json.loads(existing)
            else:
                day_data = {}

            # ✅ append (overwrite only same time)
            day_data[time_key] = final_data

            redis_client.hset(history_key, date_key, json.dumps(day_data))

            print(f"📊 OI HISTORY UPDATED → {time_key}")

        except Exception as e:
            print("OI History Error:", e)


        print("NIFTY OI saved")

    except Exception as e:
        print("OI Error:", e)    


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

def run_nifty_once(redis_client, fyers):

    candle = fetch_nifty_index_candle()

    if not candle:
        return

    update_nifty_candle_queues(redis_client, candle)
    update_nifty_day_var(redis_client, candle)
    calculate_nifty_context(redis_client)

oi_done_after_close = False

def main():
    print("NIFTY Analytics System Started")
    
    fyers = fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        log_path=""
    )

    # Robust Initialization from history
    initialize_nifty_analytics(redis_client, fyers)
    # Calculate context even if market is closed
    context_updates = calculate_nifty_context(redis_client)
    print("Initial Context:", context_updates)
    
    while True:
        now = datetime.datetime.now()
        
        # Only run during market hours (plus a small buffer)
        if is_market_open():
            # Align with 5-minute interval
            if now.minute % 5 == 0 and now.second < 10:
                print(f"[{now}] Processing NIFTY candle...")
                run_nifty_oi_once(redis_client, fyers)
                candle = fetch_nifty_index_candle()
                
                if candle:
                    # Update queues and daily variables
                    update_nifty_candle_queues(redis_client, candle)
                    updates = update_nifty_day_var(redis_client, candle)
                    context_updates = calculate_nifty_context(redis_client)

                    print(f"Day Vars: {updates}")
                    print(f"Context Vars: {context_updates}")
                    store_oi_feature_snapshot(redis_client)

                else:
                    print("No candle data available.")
                    
                # Sleep to avoid multiple processing in same minute
                time.sleep(60)
    

        else:
            global oi_done_after_close   # 👈 IMPORTANT

            # 🔥 Run OI once after market close
            if not oi_done_after_close:
                print(f"[{now}] Market closed → saving final OI...")

                run_nifty_oi_once(redis_client, fyers)

                oi_done_after_close = True

                print("🛑 Final OI saved. Stopping system.")
                break   # ✅ STOP LOOP

            # (optional) still keep heartbeat if needed before break
            time.sleep(5)
        
        time.sleep(1)


if __name__ == "__main__":
    main()