import os
import json
import time
import datetime
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
import redis
import schedule
from nifty_analytics import run_nifty_once
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
        prev_p_struct = 0

        if metrics_raw:
            prev_metrics = json.loads(metrics_raw)
            prev_parti_intra_low = prev_metrics.get("parti_intra_low", 0)
            prev_parti_intra_high = prev_metrics.get("parti_intra_high", 0)
            prev_p_struct = prev_metrics.get("p_positional", 0)

        parti_intra_low = 0
        parti_intra_high = 0
        p_positional = 0

        for stock, mc, weight in r35:

            stock_raw = redis_client.get(stock)
            if not stock_raw:
                continue

            data = json.loads(stock_raw)

            LTP = data["close"]
            day_low = data["day_low"]
            day_high = data["day_high"]
            y_close = data.get("yesterday_close")

            if y_close:
                pos_threshold = y_close + (delta * y_close)
                if LTP > pos_threshold:
                    p_positional += 1

            low_threshold = day_low + (delta * day_low)
            high_threshold = day_high - (delta * day_high)

            if LTP > low_threshold:
                parti_intra_low += 1

            if LTP < high_threshold:
                parti_intra_high += 1
        
        total_stocks = len(r35)

        parti_intra_low = round((parti_intra_low / total_stocks) * 100, 2)
        parti_intra_high = round((parti_intra_high / total_stocks) * 100, 2)
        p_positional = round((p_positional / total_stocks) * 100, 2)
        
        # acceleration
        acc_low = parti_intra_low - prev_parti_intra_low
        acc_high = parti_intra_high - prev_parti_intra_high
        a_struct = p_positional - prev_p_struct

        return parti_intra_low, parti_intra_high, acc_low, acc_high, p_positional, a_struct

    except Exception as e:
        print("Participation Error:", e)
        return 0,0,0,0,0


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
        parti_intra_low, parti_intra_high, acc_low, acc_high, p_positional, a_struct = calculate_r35_participation_and_acceleration()

        metrics = {
            "t15_strength": round(t15_strength,4),
            "r35_strength": round(r35_strength,4),
            "momentum": round(momentum,4),
            "cap_momentum": round(cap_momentum,4),
            "normalize_momentum": round(normalize_mon,2),
            "parti_intra_low": parti_intra_low,
            "parti_intra_high": parti_intra_high,
            "acc_low": acc_low,
            "acc_high": acc_high,
            "p_positional": p_positional,
            "a_struct": a_struct
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
# Calculate Market Regime
# -------------------------------
def calculate_market_regime():
    """
    Computes regime scores and appends into market_metrics
    """

    try:
        # -------------------------------
        # Load existing metrics
        # -------------------------------
        metrics_raw = redis_client.get("market_metrics")
        if not metrics_raw:
            return
        
        metrics = json.loads(metrics_raw)

        # -------------------------------
        # Load NIFTY context
        # -------------------------------
        context = redis_client.hgetall("NIFTY_CONTEXT_VAR")

        if context:
            pos = float(context.get("pos", 0))
            slope = float(context.get("slope", 0))
            trend_up = int(context.get("trend_up", 0))
            trend_down = int(context.get("trend_down", 0))
            rec_ratio = float(context.get("rec_ratio", 0))
            top_recently = int(context.get("top_recently", 0))
        else:
            pos = slope = rec_ratio = 0
            trend_up = trend_down = top_recently = 0

        # -------------------------------
        # Market breadth
        # -------------------------------
        p_struct = metrics.get("p_positional", 0)
        p_intra_low = metrics.get("parti_intra_low", 0)
        p_intra_high = metrics.get("parti_intra_high", 0)

        # -------------------------------
        # 1) Downtrend
        # -------------------------------
        score_down = (
            20 * (pos < 0.35) +
            20 * (trend_down == 1) +
            20 * (slope < 0) +
            20 * (p_struct < 45) +
            20 * (p_intra_high > 55)
        )

        # -------------------------------
        # 2) Recovery
        # -------------------------------
        score_recovery = (
            20 * (rec_ratio > 0.60) +
            20 * (pos > 0.50) +
            20 * (slope > 0) +
            20 * (p_intra_low > 55) +
            20 * (p_struct > 50)
        )

        # -------------------------------
        # 3) Uptrend
        # -------------------------------
        score_up = (
            20 * (pos > 0.65) +
            20 * (trend_up == 1) +
            20 * (slope > 0) +
            20 * (p_struct > 55) +
            20 * (p_intra_low > 55)
        )

        # -------------------------------
        # 4) Breakdown
        # -------------------------------
        score_breakdown = (
            20 * (top_recently == 1) +
            20 * (pos < 0.55) +
            20 * (slope < 0) +
            20 * (p_intra_high > 55) +
            20 * (p_struct < 50)
        )

        # -------------------------------
        # 5) Chop
        # -------------------------------
        score_chop = (
            20 * (0.35 < pos < 0.65) +
            20 * (abs(slope) < 5) +
            20 * (abs(metrics.get("momentum", 0)) < 5) +
            20 * (45 < p_struct < 55) +
            20 * (abs(metrics.get("t15_strength", 0)) < 1)
        )

        # -------------------------------
        # Final Regime
        # -------------------------------
        regime_scores = {
            "downtrend": score_down,
            "recovery": score_recovery,
            "uptrend": score_up,
            "breakdown": score_breakdown,
            "chop": score_chop
        }

        
        # -------------------------------
        # Append into metrics (matrix)
        # -------------------------------
        metrics.update({
            "score_down": score_down,
            "score_recovery": score_recovery,
            "score_up": score_up,
            "score_breakdown": score_breakdown,
            "score_chop": score_chop,
            
        })

        # ================================
        # CI CALCULATION 
        # ================================

        M = metrics.get("normalize_momentum", 0)

        p_struct = metrics.get("p_positional", 0)
        p_intra_low = metrics.get("parti_intra_low", 0)
        p_intra_high = metrics.get("parti_intra_high", 0)

        a_struct = metrics.get("a_struct", 0)
        a_intra_low = metrics.get("acc_low", 0)
        a_intra_high = metrics.get("acc_high", 0)

        # -------------------------------
        # CI PER REGIME
        # -------------------------------

        ci_down = 0.5*M + 0.3*p_struct + 0.2*a_struct

        ci_recovery = 0.5*M + 0.3*p_intra_low + 0.2*a_intra_low

        ci_up = 0.5*M + 0.3*p_struct + 0.2*a_struct

        ci_breakdown = 0.5*M + 0.3*(100 - p_intra_high) + 0.2*(100 - a_intra_high)

        # Chop special
        p_chop = 0.5*p_struct + 0.5*p_intra_low
        ci_chop = 0.5*M + 0.3*p_chop + 0.2*50

        # -------------------------------
        # WEIGHTS (PROBABILITIES)
        # -------------------------------

        total_score = score_down + score_recovery + score_up + score_breakdown + score_chop

        if total_score == 0:
            w_down = w_rec = w_up = w_brk = w_chop = 0
        else:
            w_down = score_down / total_score
            w_rec = score_recovery / total_score
            w_up = score_up / total_score
            w_brk = score_breakdown / total_score
            w_chop = score_chop / total_score

        # -------------------------------
        # FINAL CI (WEIGHTED 🔥)
        # -------------------------------

        ci_final = (
            w_down * ci_down +
            w_rec * ci_recovery +
            w_up * ci_up +
            w_brk * ci_breakdown +
            w_chop * ci_chop
        )

        # -------------------------------
        # ADD TO METRICS (MATRIX)
        # -------------------------------

        metrics.update({
            "ci_down": round(ci_down, 2),
            "ci_recovery": round(ci_recovery, 2),
            "ci_up": round(ci_up, 2),
            "ci_breakdown": round(ci_breakdown, 2),
            "ci_chop": round(ci_chop, 2),

            "w_down": round(w_down, 3),
            "w_recovery": round(w_rec, 3), 
            "w_up": round(w_up, 3),
            "w_breakdown": round(w_brk, 3),
            "w_chop": round(w_chop, 3),

            "ci_final": round(ci_final, 2)
        })

        # -------------------------------
        # Save back
        # -------------------------------
        redis_client.set("market_metrics", json.dumps(metrics))

        print("Regime:", regime_scores)

    except Exception as e:
        print("Regime Error:", e)

def store_market_history():
    try:
        # -----------------------------
        # LOAD MARKET METRICS
        # -----------------------------
        metrics_raw = redis_client.get("market_metrics")
        if not metrics_raw:
            return

        metrics = json.loads(metrics_raw)

        # -----------------------------
        # LOAD NIFTY DATA
        # -----------------------------
        day_vars = redis_client.hgetall("NIFTY_DAY_VAR")
        context_vars = redis_client.hgetall("NIFTY_CONTEXT_VAR")

        # ✅ CORRECT WAY
        sample_stock = redis_client.get("RELIANCE")
        if not sample_stock:
            return

        sample_data = json.loads(sample_stock)
        candle_ts = sample_data.get("timestamp")

        if not candle_ts:
            return

        candle_dt = datetime.datetime.fromtimestamp(candle_ts)

        date_key = candle_dt.strftime("%Y-%m-%d")
        time_key = candle_dt.strftime("%H:%M")

        redis_key = "market_history"

        existing = redis_client.hget(redis_key, date_key)

        if existing:
            day_data = json.loads(existing)
        else:
            day_data = {}

        # -----------------------------
        # COMBINED DATA 🔥
        # -----------------------------
        combined = {

            # ===== MARKET =====
            "t15_strength": metrics.get("t15_strength"),
            "r35_strength": metrics.get("r35_strength"),
            "momentum": metrics.get("momentum"),
            "normalize_momentum": metrics.get("normalize_momentum"),

            "parti_intra_low": metrics.get("parti_intra_low"),
            "parti_intra_high": metrics.get("parti_intra_high"),
            "acc_low": metrics.get("acc_low"),
            "acc_high": metrics.get("acc_high"),
            "p_positional": metrics.get("p_positional"),
            "a_struct": metrics.get("a_struct"),

            "score_down": metrics.get("score_down"),
            "score_recovery": metrics.get("score_recovery"),
            "score_up": metrics.get("score_up"),
            "score_breakdown": metrics.get("score_breakdown"),
            "score_chop": metrics.get("score_chop"),

            "ci_down": metrics.get("ci_down"),
            "ci_recovery": metrics.get("ci_recovery"),
            "ci_up": metrics.get("ci_up"),
            "ci_breakdown": metrics.get("ci_breakdown"),
            "ci_chop": metrics.get("ci_chop"),
            "ci_final": metrics.get("ci_final"),

            "w_down": metrics.get("w_down"),
            "w_recovery": metrics.get("w_recovery"),
            "w_up": metrics.get("w_up"),
            "w_breakdown": metrics.get("w_breakdown"),
            "w_chop": metrics.get("w_chop"),

            # ===== NIFTY (NEW 🔥) =====
            "nifty_close": float(day_vars.get("close", 0)),
            "nifty_ema5": float(day_vars.get("EMA5", 0)),
            "nifty_ema20": float(day_vars.get("EMA20", 0)),

            "nifty_pos": float(context_vars.get("pos", 0)),
            "nifty_slope": float(context_vars.get("slope", 0)),
            "nifty_trend_up": int(context_vars.get("trend_up", 0)),
            "nifty_trend_down": int(context_vars.get("trend_down", 0)),

            "nifty_rec_ratio": float(context_vars.get("rec_ratio", 0)),
            "nifty_top_recently": int(context_vars.get("top_recently", 0)),
        }

        # -----------------------------
        # APPEND
        # -----------------------------
        day_data[time_key] = combined

        redis_client.hset(redis_key, date_key, json.dumps(day_data))

        print(f"Saved FULL market + nifty → {date_key} {time_key}")

    except Exception as e:
        print("History Store Error:", e)        

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

def initialize_stock_day_levels(symbol, fyers):
    """
    Fetch full day history and calculate correct day_high, day_low, day_open
    """

    today = datetime.datetime.now().strftime("%Y-%m-%d")

    data = {
        "symbol": symbol,
        "resolution": "5",
        "date_format": "1",
        "range_from": today,
        "range_to": today,
        "cont_flag": "1"
    }

    try:
        response = fyers.history(data)

        if "candles" not in response or not response["candles"]:
            return None

        candles = response["candles"]

        day_open = candles[0][1]
        day_high = max(c[2] for c in candles)
        day_low = min(c[3] for c in candles)

        return {
            "day_open": day_open,
            "day_high": day_high,
            "day_low": day_low
        }

    except Exception as e:
        print(f"Init Error ({symbol}):", e)
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

                        if existing.get("yesterday_close") is None:
                            levels = fetch_day_levels(symbol, fyers)
                            candle["yesterday_close"] = levels["yesterday_close"] if levels else None
                        else:
                            candle["yesterday_close"] = existing["yesterday_close"]

                    else:

                        levels = initialize_stock_day_levels(symbol, fyers)

                        if levels:
                            candle["day_high"] = levels["day_high"]
                            candle["day_low"] = levels["day_low"]
                        else:
                            candle["day_high"] = candle["high"]
                            candle["day_low"] = candle["low"]

                        # yesterday close
                        q_levels = fetch_day_levels(symbol, fyers)
                        candle["yesterday_close"] = q_levels["yesterday_close"] if q_levels else None
                        
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

    # Calculate market regime
    calculate_market_regime()

    # Store market history
    store_market_history()

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