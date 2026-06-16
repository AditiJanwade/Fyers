import datetime
import json
import math

class NiftyAnalytics:
    
    DAY_VAR = "NIFTY_DAY_VAR"
    LOCK_KEY = "NIFTY_OI_LOCK"

    def __init__(self, redis_client, fyers):
        self.redis = redis_client
        self.fyers = fyers

        # Runtime cache
        self.candle = None
        self.day_vars = {}
        self.oi_data = None
        self.features = None

        self.date_key = None
        self.time_key = None
        self.fut_open = None
        self.fut_price = None
        self.oi_history = {}
        self.feature_history = {}
        pass

    def initialize_nifty_analytics(self):
        """
        Seeds Redis queues and daily variables from historical data at startup.
        This fixes issues with late starts and restarts.
        """
        print("Initialising NIFTY Analytics from history...")
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        
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
            response = self.fyers.history(data)
            if "candles" not in response or not response["candles"]:
                print("No historical candles found for initialization.")
                return

            candles = response["candles"]
            today_candles = [
                c for c in candles
                if datetime.datetime.fromtimestamp(
                    c[0]
                ).strftime("%Y-%m-%d") == today_str
            ]

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

                self.day_vars = updates
        except Exception as e:
                print(
                    f"Initialization Error: {e}"
                )    

    def fetch_nifty_index_candle(self):
        """
        Fetches the latest 5-minute candle for NIFTY 50-INDEX.
        """
        
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
            response = self.fyers.history(data)
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
        
    def update_nifty_day_var(self, candle):
        """
        Updates daily NIFTY variables in Redis.
        Handles daily reset and updates high/low/close/EMAs.
        """
        candle_time = datetime.datetime.fromtimestamp(candle['timestamp'])
        candle_date_str = candle_time.strftime("%Y-%m-%d")
        candle_time_str = candle_time.strftime("%H:%M")
        
        stored_vars = self.day_vars
        last_date = stored_vars.get("date")
        
        updates = stored_vars.copy()
        
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
            
            updates["day_high"] = max(
                current_high,
                candle["high"]
            )

            updates["day_low"] = min(
                current_low,
                candle["low"]
            )
                
            if "day_open" not in stored_vars and candle_time_str == "09:15":
                updates["day_open"] = candle["open"]

        # Always update current close
        updates["close"] = candle["close"]
        self.day_vars = updates

        return updates
    
    def get_nifty_candle(self):

        candle = self.fetch_nifty_index_candle()

        if not candle:
            return None

        self.candle = candle

        self.day_vars = self.update_nifty_day_var(candle)

        candle_time = datetime.datetime.fromtimestamp(
            candle["timestamp"]
        )

        self.date_key = candle_time.strftime("%Y-%m-%d")
        self.time_key = candle_time.strftime("%H:%M")

        return candle  
    
    def get_nifty_future(self):

        try:

            now = datetime.datetime.now()

            months = [
                "JAN","FEB","MAR","APR","MAY","JUN",
                "JUL","AUG","SEP","OCT","NOV","DEC"
            ]

            year = str(now.year)[-2:]

            curr_idx = now.month - 1

            symbols_to_try = []

            curr_month = months[curr_idx]
            next_month = months[(curr_idx + 1) % 12]

            symbols_to_try.append(
                f"NSE:NIFTY{year}{curr_month}FUT"
            )

            symbols_to_try.append(
                f"NSE:NIFTY{year}{next_month}FUT"
            )

            for fut_symbol in symbols_to_try:

                response = self.fyers.quotes(
                    {"symbols": fut_symbol}
                )

                if "d" in response and response["d"]:

                    data = response["d"][0].get(
                        "v",
                        {}
                    )

                    price = (
                        data.get("lp")
                        or data.get("last_price")
                        or 0
                    )

                    if price > 0:
                        return float(price)

        except Exception as e:
            print(
                f"Future Price Error: {e}"
            )

        return 0
    
    def get_or_create_lock(self, options, fut_price):
        lock_data = self.redis.get(self.LOCK_KEY)
        # -----------------------------
        # GET DAY VARS
        # -----------------------------
        today_date = self.day_vars.get("date")

        if lock_data:
            lock = json.loads(lock_data)

            lock_date = lock.get("date")   # 👈 ADD THIS

            # ✅ RESET IF NEW DAY
            if lock_date != today_date:
                print("🆕 NEW DAY → RESETTING OI LOCK")
                self.redis.delete(self.LOCK_KEY)

            # ✅ RESET if 500 move
            elif abs(fut_price - lock["base_price"]) >= 500:
                print("🔁 500 MOVE → RESETTING LOCK")
                self.redis.delete(self.LOCK_KEY)

            else:
                return lock

        # -----------------------------
        # CREATE NEW LOCK
        # -----------------------------
        base_price = (
            self.fut_open
            if self.fut_open
            else fut_price
        )

        strikes = sorted(set(
            item["strike_price"]
            for item in options
            if item["strike_price"] != -1
        ))

        atm_index = min(range(len(strikes)), key=lambda i: abs(strikes[i] - base_price))

        selected_strikes = strikes[max(0, atm_index - 10): atm_index + 11]

        lock = {
            "base_price": base_price,
            "strikes": selected_strikes,
            "date": today_date   # 👈 STORE DATE
        }

        self.redis.set(
            self.LOCK_KEY,
            json.dumps(lock)
        )

        print(f"🔒 Lock Created at OPEN {round(base_price)}")

        return lock          
            
    def get_nifty_oi(self):
        data = {
            "symbol": "NSE:NIFTY50-INDEX",
            "strikecount": 50,
            "timestamp": "",
            "greeks": 1 
        }
        fut_price = self.get_nifty_future()

        self.fut_price = fut_price

        if self.fut_open is None:
            self.fut_open = fut_price

        try:
            response = self.fyers.optionchain(data=data)

            if response["code"] != 200:
                print("OI fetch error")
                return

            raw = response["data"]
            options = raw["optionsChain"]

            # -----------------------------
            # 1. LATEST EXPIRY
            # -----------------------------
            expiry_list = raw.get("expiryData", [])
            latest_expiry = expiry_list[0]["date"]
            nifty_ltp = next(
                item["ltp"]
                for item in options
                if item["strike_price"] == -1
            )

            # 🔒 LOCK STRIKES
            lock = self.get_or_create_lock(options,fut_price)
            selected_strikes = lock["strikes"]

            # -----------------------------
            # 3. FILTER CE & PE
            # -----------------------------
            ce_data = {}
            pe_data = {}
            ce_iv = {}
            pe_iv = {}
            ce_ltp = {}
            pe_ltp = {}
            
            for item in options:
                strike = item["strike_price"]

                if strike in selected_strikes:

                    if item["option_type"] == "CE":
                        ce_data[strike] = item["oi"]
                        ce_iv[strike] = item.get("greeks", {}).get("iv", 0)
                        ce_ltp[strike] = item.get("ltp", 0)

                    elif item["option_type"] == "PE":
                        pe_data[strike] = item["oi"]
                        pe_iv[strike] = item.get("greeks", {}).get("iv", 0)
                        pe_ltp[strike] = item.get("ltp", 0)
            
            final_data = {
                "expiry": latest_expiry,
                "ltp": nifty_ltp,
                "base_price": lock["base_price"],
                "strikes": selected_strikes,
                "CE": ce_data,
                "PE": pe_data,
                "CE_IV": ce_iv,
                "PE_IV": pe_iv,
                "CE_LTP": ce_ltp,
                "PE_LTP": pe_ltp,
                "timestamp": datetime.datetime.now().strftime("%H:%M:%S")
            }
            self.oi_data = final_data

            print("NIFTY OI saved")
            if self.time_key:
                self.oi_history[self.time_key] = final_data

            return final_data

        except Exception as e:
            print("OI Error:", e)    

    def build_oi_feature_snapshot(self):
        try:
            
            # -------------------------
            # OI DATA
            # -------------------------
            oi_data = self.oi_data

            if not oi_data:
                return None

            pe_data = {int(k): v for k, v in oi_data.get("PE", {}).items()}
            ce_data = {int(k): v for k, v in oi_data.get("CE", {}).items()}
            strikes = oi_data.get("strikes", [])
            expiry = oi_data.get("expiry")

            # TOP 2 CE STRIKES
            sorted_ce = sorted(
                ce_data.items(),
                key=lambda x: x[1],
                reverse=True
            )

            top_ce_strike = int(sorted_ce[0][0]) if len(sorted_ce) > 0 else 0
        
            
            # TOP 2 PE STRIKES
            sorted_pe = sorted(
                pe_data.items(),
                key=lambda x: x[1],
                reverse=True
            )

            top_pe_strike = int(sorted_pe[0][0]) if len(sorted_pe) > 0 else 0
            
            if not strikes:
                return None
            
            nifty_price = float(
                self.day_vars.get("close", 0)
            )

            fut_price = self.fut_price

            diff = fut_price - nifty_price      

            # -------------------------
            # SUPPORT / RESISTANCE (Directional)
            # -------------------------        

            atm_index = min(
                range(len(strikes)),
                key=lambda i: abs(strikes[i] - fut_price)
            )
            atm_strike = strikes[atm_index]

            atm_ce_iv = 0
            atm_pe_iv = 0
            atm_ce_ltp = 0
            atm_pe_ltp = 0
            atm_straddle = 0

            ce_iv = oi_data.get("CE_IV", {})
            pe_iv = oi_data.get("PE_IV", {})

            ce_ltp = oi_data.get("CE_LTP", {})
            pe_ltp = oi_data.get("PE_LTP", {})

            atm_ce_ltp = ce_ltp.get(atm_strike, 0)
            atm_pe_ltp = pe_ltp.get(atm_strike, 0)

            atm_straddle = atm_ce_ltp + atm_pe_ltp

            atm_ce_iv = ce_iv.get(atm_strike, 0)
            atm_pe_iv = pe_iv.get(atm_strike, 0)

            Exp_Expiry = atm_straddle / 1.25

            # Expiry date from OI data
            expiry_date = datetime.datetime.strptime(expiry, "%d-%m-%Y").date()

            today = datetime.date.today()

            # Expiry Day
            if expiry_date == today:

                now = datetime.datetime.now()

                market_start = now.replace(
                    hour=9,
                    minute=15,
                    second=0,
                    microsecond=0
                )

                market_end = now.replace(
                    hour=15,
                    minute=30,
                    second=0,
                    microsecond=0
                )

                total_minutes = 375  # 9:15 → 15:30

                remaining_minutes = max(
                    1,
                    (market_end - now).total_seconds() / 60
                )

                Exp_Intra = Exp_Expiry * math.sqrt(
                    remaining_minutes / total_minutes
                )

            else:

                days_to_expiry = max(
                    1,
                    (expiry_date - today).days
                )

                Exp_Intra = Exp_Expiry * math.sqrt(
                    1 / days_to_expiry
                )
            
            # CE → Resistance side (ATM to ATM + 3)
            ce_strikes = strikes[atm_index : min(len(strikes), atm_index + 4)]

            # PE → Support side (ATM to ATM - 3)
            pe_strikes = strikes[max(0, atm_index - 3) : atm_index + 1]

            # Local OI sums
            support_sum = sum(float(pe_data.get(s, 0)) for s in pe_strikes)
            resistance_sum = sum(float(ce_data.get(s, 0)) for s in ce_strikes)

            # -------------------------
            # PCR & OI BIAS
            # -------------------------
            pcr = (support_sum / resistance_sum ) if resistance_sum else 0
            # oi_bias = support - resistance

            denominator = support_sum + resistance_sum

            if denominator == 0:
                oi_bias = 0
            else:
                oi_bias = ((resistance_sum - support_sum) / denominator) * 100

            # ---------------------------------
            # FULL LOCKED STRIKE (20/21) VALUES
            # ---------------------------------

            all_strikes = strikes

            support_20 = sum(float(pe_data.get(s, 0)) for s in all_strikes)

            resistance_20 = sum(float(ce_data.get(s, 0)) for s in all_strikes)

            pcr_20 = support_20 / resistance_20 if resistance_20 else 0

            denominator_20 = support_20 + resistance_20

            if denominator_20 == 0:
                oi_bias_20 = 0
            else:
                oi_bias_20 = ((resistance_20 - support_20) / denominator_20) * 100    


            # -------------------------
            # FINAL FEATURE OBJECT
            # -------------------------
            features = {
                "diff":diff,
                "support_sum": round(support_sum, 2),
                "resistance_sum": round(resistance_sum, 2),
                "oi_bias": round(oi_bias, 2),
                "pcr": round(pcr, 2),
                "support_20": round(support_20, 2),
                "resistance_20": round(resistance_20, 2),
                "oi_bias_20": round(oi_bias_20, 2),
                "pcr_20": round(pcr_20, 2),
                "nifty_price": nifty_price,
                "nifty_fut_price": fut_price,
                "top_ce_strike": top_ce_strike,
                "top_pe_strike": top_pe_strike,

                "CE_IV":atm_ce_iv,
                "PE_IV":atm_pe_iv,

                "ATM_STRADDLE": round(atm_straddle, 2),
                "EXP_MOVE_EXPIRY": round(Exp_Expiry, 2),
                "EXP_MOVE_INTRADAY": round(Exp_Intra, 2),
            }
            self.features = features
            return features

        except Exception as e:
            print("Feature Build Error:", e)
            return None

    def store_oi_feature_snapshot(self):

        try:

            features = self.build_oi_feature_snapshot()

            if not features:
                return

            # -------------------------
            # TODAY RUNNING AVERAGES
            # -------------------------

            prev_diff = [
                v.get("diff", 0)
                for v in self.feature_history.values()
            ]

            prev_iv = [
                v.get("CE_IV", 0)
                for v in self.feature_history.values()
            ]

            prev_straddle = [
                v.get("ATM_STRADDLE", 0)
                for v in self.feature_history.values()
            ]

            features["FUT_PREMIUM_AVG"] = round(
                (
                    sum(prev_diff)
                    + features["diff"]
                ) /
                (len(prev_diff) + 1),
                2
            )

            features["ATM_IV_AVG"] = round(
                (
                    sum(prev_iv)
                    + features["CE_IV"]
                ) /
                (len(prev_iv) + 1),
                2
            )

            features["ATM_STRADDLE_AVG"] = round(
                (
                    sum(prev_straddle)
                    + features["ATM_STRADDLE"]
                ) /
                (len(prev_straddle) + 1),
                2
            )

            features["ATM_UPPER"] = round(
                features["ATM_IV_AVG"] * 1.2,
                2
            )

            features["ATM_LOWER"] = round(
                features["ATM_IV_AVG"] * 0.8,
                2
            )

            self.features = features

            self.feature_history[
                self.time_key
            ] = features

            print("-------------features--------------")

            for key, value in features.items():
                print(f"{key}: {value}")

            print("-----------------------------------")

            print(
                f"✅ OI Feature Cached → "
                f"{self.time_key}"
            )

        except Exception as e:

            print(
                "OI Store Error:",
                e
            )

    def save_all(self):

        try:

            pipe = self.redis.pipeline()

            # ----------------------------------
            # DAY VARS
            # ----------------------------------
            if self.day_vars:

                pipe.hset(
                    self.DAY_VAR,
                    mapping=self.day_vars
                )

            # ----------------------------------
            # CURRENT OI SNAPSHOT
            # ----------------------------------
            if self.oi_data:

                pipe.set(
                    "NIFTY_OI_20",
                    json.dumps(self.oi_data)
                )

            # ----------------------------------
            # OI HISTORY
            # ----------------------------------
            if self.oi_history:

                pipe.hset(
                    "NIFTY_OI_HISTORY",
                    self.date_key,
                    json.dumps(self.oi_history)
                )

            # ----------------------------------
            # CURRENT FEATURE SNAPSHOT
            # ----------------------------------
            if self.features:

                pipe.set(
                    "NIFTY_FEATURE_LATEST",
                    json.dumps(self.features)
                )

            # ----------------------------------
            # FEATURE HISTORY
            # ----------------------------------
            if self.feature_history:

                pipe.hset(
                    "OI_FEATURE_LIVE",
                    self.date_key,
                    json.dumps(self.feature_history)
                )

            
            # ----------------------------------
            # EXECUTE ONCE
            # ----------------------------------
            pipe.execute()

            print(
                f"✅ All Data Saved → "
                f"{self.time_key}"
            )

        except Exception as e:

            print(
                "Save All Error:",
                e
            )        