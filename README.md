## 🧠 How the System Works (End-to-End Flow)

This system is a **real-time market analytics engine** that runs continuously during market hours and processes data every **5 minutes (candle-based execution)**.

---

### 🚀 1. System Start

Run the project:

```bash
python main.py
```

#### Step 1: Login

* Executes `fyers_auto_login.py`
* Generates **ACCESS_TOKEN**
* Saves credentials in `.env`

---

#### Step 2: Initialize NIFTY

* Calls `initialize_nifty_analytics()`
* Loads historical data from Fyers
* Sets:

  * `day_open`
  * `day_high`
  * `day_low`
  * `EMA5`, `EMA20`

---

### 🔁 2. First Cycle Execution

Immediately after initialization:

```python
run_cycle()
```

---

### ⚙️ Inside `run_cycle()`

#### 1️⃣ Update NIFTY Data

* Function: `run_nifty_once()`
* Fetches latest NIFTY candle
* Updates:

  * EMA
  * Trend (up/down)
  * Slope
  * Position in range

---

#### 2️⃣ Fetch Options OI Data

* Function: `run_nifty_oi_once()`
* Fetches option chain from Fyers
* Filters strikes around ATM

---

##### 🔒 OI Lock Logic

* Base price = **day_open**
* Locks nearby strikes
* Resets if price moves ≥ 500 points

---

#### 3️⃣ Fetch NIFTY 50 Stock Data

* Function: `fetch_and_store_data()`

For each stock:

* Fetch latest 5-min candle
* Store in Redis
* Update:

  * `day_high`
  * `day_low`
  * `yesterday_close`

---

#### 4️⃣ Market Intelligence Computation

##### 📊 Market Cap Split

* Top 15 → **T15**
* Remaining 35 → **R35**

---

##### 📈 Strength Calculation

* Function: `calculate_group_strength()`
* Outputs:

  * T15 strength
  * R35 strength
  * Momentum

---

##### 📉 Market Regime Detection

* Function: `calculate_market_regime()`
* Detects:

  * Uptrend
  * Downtrend
  * Recovery
  * Breakdown
  * Chop (sideways)

---

##### ⚡ Confidence Index (CI)

* Combines:

  * Momentum
  * Participation
  * Acceleration
* Produces a single **market confidence score**

---

#### 5️⃣ Store Market History

* Function: `store_market_history()`
* Stores:

  * Market metrics
  * NIFTY context
  * Time-based snapshots

---

#### 6️⃣ Build OI Features & Signals

* Function: `store_oi_feature_snapshot()`

Generates:

```json
{
  "ci": 62,
  "support": 26,
  "resistance": 24,
  "oi_bias": 2,
  "pcr": 0.8
}
```

---

##### 🔔 Alert Logic

* `oi_bias ≥ 15` → **Bullish Signal 🚀**
* `oi_bias ≤ -15` → **Bearish Signal 🔻**

---

### 🔁 3. Continuous Engine Loop

* Function: `start_engine()`
* Runs continuously during market hours

#### ⏱️ Execution Rule

```python
if current_time % 5 minutes:
    run_cycle()
```

👉 Ensures perfect alignment with market candles

---

### 🛑 4. Market Close Handling

At **3:35 PM**:

* Function: `run_final()`
* Executes:

  * Final OI calculation
  * Final NIFTY update
  * Saves full day data to MongoDB
* Stops system

---

## 🗄️ Data Flow Architecture

### 🔴 Redis (Real-time Layer)

Stores:

* `NIFTY_DAY_VAR` → Live NIFTY state
* `NIFTY_CONTEXT_VAR` → Trend, slope, position
* `market_metrics` → Strength, CI
* `NIFTY_OI_20` → OI data
* `OI_FEATURE_LIVE` → Signals
* `market_history` → Time-series snapshots

---

### 🟢 MongoDB (Storage Layer)

Stores:

* Full-day structured data (`nifty_full_day`)

---

## ⚡ Complete Pipeline

```
Fyers API → Redis → Analytics → OI Logic → CI → Alerts → MongoDB
```

---

## 💡 Summary

* Runs every **5 minutes (candle-based)**
* Combines:

  * Market breadth
  * Trend analysis
  * Options data
* Generates:

  * Support / Resistance
  * CI score
  * Bullish / Bearish signals

👉 This is a **real-time quantitative trading analytics system**
