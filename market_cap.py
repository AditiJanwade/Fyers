import redis
import json
from dotenv import load_dotenv
import os
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
# CONNECT REDIS SERVER
try:
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True
    )
    r.ping()
    print("Connected to Redis for Market Cap calculation.")
except Exception as e:
    print(f"Redis Connection Error: {e}")
    exit()

# READ FLOATING SHARES FROM FILE
with open("floating_shares.json", "r") as f:
    floating_shares = json.load(f)

print(f"Calculating market caps for {len(floating_shares)} stocks...")
market_caps = []

# FETCH DATA FROM REDIS
for stock, shares in floating_shares.items():

    key = f"{stock}"

    data = r.get(key)

    if data is None:
        continue

    candle = json.loads(data)

    close_price = candle["close"]

    market_cap = close_price * shares

    market_caps.append((stock, market_cap))


# SORT DESCENDING
market_caps.sort(key=lambda x: x[1], reverse=True)

# SPLIT
T15 = tuple(market_caps[:15])
R35 = tuple(market_caps[15:])

# STORE IN REDIS
r.set("T15", json.dumps(T15))
r.set("R35", json.dumps(R35))

print("✅ Redis Updated")