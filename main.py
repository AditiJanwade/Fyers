import time
import datetime
import subprocess
import sys
import os
import redis
import json 
from dotenv import load_dotenv
from fyers_apiv3 import fyersModel
from nifty_analytics import run_nifty_oi_once
from fetch_nifty_redis import fetch_and_store_data
from nifty_analytics import run_nifty_once, initialize_nifty_analytics,store_oi_feature_snapshot
from pymongo import MongoClient


# -------------------------------
# MONGODB
# -------------------------------
MONGO_URI = os.getenv("MONGO_URI")

if not MONGO_URI:
    raise ValueError("MONGO_URI not found in .env ❌")

mongo_client = MongoClient(MONGO_URI)

mongo_db = mongo_client["market_data"]      # database name
mongo_collection = mongo_db["nifty_eod"]    # collection name

print("Connected to MongoDB ")


# -------------------------------
# ENV + REDIS
# -------------------------------
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=True
)


# -------------------------------
# MONGODB SAVE FUNCTION
# -------------------------------
def save_today_to_mongo():
    print("\n🔄 Syncing FULL DATA (Missing Dates Only) → MongoDB")

    try:
        collection = mongo_db["nifty_full_day"]

        # -----------------------------
        # 1. REDIS DATES (from candles)
        # -----------------------------
        redis_dates = redis_client.hkeys("market_history")

        if not redis_dates:
            print("No Redis data ❌")
            return

        redis_dates = sorted(redis_dates)

        # -----------------------------
        # 2. MONGO DATES
        # -----------------------------
        mongo_dates = set()

        for doc in collection.find({}, {"date": 1}):
            mongo_dates.add(doc["date"])

        print("Mongo Dates:", mongo_dates)

        # -----------------------------
        # 3. FIND MISSING
        # -----------------------------
        missing_dates = [d for d in redis_dates if d not in mongo_dates]

        if not missing_dates:
            print("✅ Mongo already up-to-date")
            return

        print("📌 Missing Dates:", missing_dates)

        # -----------------------------
        # 4. SAVE EACH MISSING DATE
        # -----------------------------
        for date in missing_dates:

            candles_raw = redis_client.hget("market_history", date)
            oi_raw = redis_client.hget("NIFTY_OI_HISTORY", date)
            features_raw = redis_client.hget("OI_FEATURE_LIVE", date)

            if not candles_raw:
                print(f"No candle data for {date}")
                continue

            if not oi_raw:
                print(f"No OI data for {date}")
                continue

            if not features_raw:
                print(f"No features data for {date}")
                continue

            candles = json.loads(candles_raw)
            oi_data = json.loads(oi_raw) if oi_raw else {}
            features = json.loads(features_raw) if features_raw else {}

            # sort by time
            candles = dict(sorted(candles.items()))
            oi_data = dict(sorted(oi_data.items()))
            features = dict(sorted(features.items()))

            document = {
                "date": date,
                "candles": candles,
                "oi_data": oi_data,
                "features": features,
                "created_at": datetime.datetime.now()
            }

            collection.update_one(
                {"date": date},
                {"$set": document},
                upsert=True
            )

            print(f"✅ Saved FULL DATA for {date}")

        print("🎯 Sync Completed")

    except Exception as e:
        print("Mongo Sync Error:", e)

# -------------------------------
# FYERS
# -------------------------------
def get_fyers():
    return fyersModel.FyersModel(
        client_id=CLIENT_ID,
        token=ACCESS_TOKEN,
        log_path=""
    )

# -------------------------------
# LOGIN
# -------------------------------
def login():
    print("Logging in...")

    result = subprocess.run([sys.executable, "fyers_auto_login.py"])

    if result.returncode != 0:
        print("Login failed ❌")
        exit()

    print("Login successful ✅")

# -------------------------------
# INIT NIFTY
# -------------------------------
def init_nifty():
    print("Initializing NIFTY...")

    fyers = get_fyers()
    initialize_nifty_analytics(redis_client, fyers)

    print("NIFTY initialized ✅")

# -------------------------------
# WAIT FOR NEXT 5-MIN CANDLE
# -------------------------------
last_run_minute = None

def wait_for_next_candle():
    global last_run_minute

    while True:
        now = datetime.datetime.now()

        # Wait for market open (9:15)
        if now.hour < 9 or (now.hour == 9 and now.minute < 15):
            time.sleep(10)
            continue

        # Trigger exactly once per 5-min candle
        if now.minute % 5 == 0 and now.second >= 2:
            if last_run_minute != now.minute:
                last_run_minute = now.minute
                return

        time.sleep(1)

# -------------------------------
# RUN ONE CYCLE
# -------------------------------
def run_cycle():
    print(f"\nRunning cycle: {datetime.datetime.now()}")

    try:
        fyers = get_fyers()
        
        # ✅ FIRST: update candle + day_open
        run_nifty_once(redis_client, fyers)

        # ✅ THEN: OI (lock will use day_open)
        run_nifty_oi_once(redis_client, fyers)
        fetch_and_store_data()
        store_oi_feature_snapshot(redis_client) 

        print("Cycle completed ✅")

    except Exception as e:
        print(f"Cycle Error: {e}")
        time.sleep(5)

# -------------------------------
# FINAL RUN AFTER MARKET CLOSE
# -------------------------------
def run_final():
    print("\nFinal market close run")

    try:
        fyers = get_fyers()
        run_nifty_oi_once(redis_client, fyers)
        run_nifty_once(redis_client, fyers)
        fetch_and_store_data()
        save_today_to_mongo()

        print("Final cycle completed ✅")

    except Exception as e:
        print(f"Final Run Error: {e}")

    print("System stopping 🛑")

# -------------------------------
# ENGINE LOOP
# -------------------------------
def start_engine():
    print("Engine started 🚀")

    last_run_minute = None

    while True:
        now = datetime.datetime.now()

        # 🔴 Market close
        if now.hour > 15 or (now.hour == 15 and now.minute >= 35):
            run_final()
            break

        # 🔵 Run every 5-min candle (ONLY ONCE)
        if now.minute % 5 == 0 and now.second >= 2:
            if last_run_minute != now.minute:
                last_run_minute = now.minute

                run_cycle()

        time.sleep(1)

# -------------------------------
# MAIN
# -------------------------------
if __name__ == "__main__":

    print("System started 🚀")

    login()
    init_nifty()

    run_cycle()
    
    start_engine()