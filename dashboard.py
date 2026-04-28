import os
import json
from datetime import datetime

import streamlit as st
import redis
import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh

# -----------------------------------
# LOAD ENV
# -----------------------------------
load_dotenv()

# -----------------------------------
# PAGE CONFIG
# -----------------------------------
st.set_page_config(
    page_title="NIFTY OI Dashboard",
    page_icon="📈",
    layout="wide"
)
st_autorefresh(interval=30000, key="live_refresh")
# -----------------------------------
# CUSTOM CSS
# -----------------------------------
st.markdown("""
<style>
html, body, [class*="css"] {
    font-family: Arial, sans-serif;
}

[data-testid="metric-container"] {
    background: linear-gradient(135deg, #111827, #1f2937);
    border: 1px solid #374151;
    padding: 18px;
    border-radius: 14px;
    box-shadow: 0 0 8px rgba(0,0,0,0.25);
}

h1, h2, h3 {
    color: white;
}
</style>
""", unsafe_allow_html=True)

# -----------------------------------
# REDIS CONNECTION
# -----------------------------------
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=True
)

# -----------------------------------
# LOAD DATA
# -----------------------------------
today = datetime.now().strftime("%Y-%m-%d")
raw = r.hget("OI_FEATURE_LIVE", today)

if not raw:
    st.error("No Data Found in Redis.")
    st.stop()

data = json.loads(raw)

rows = []
for t, v in data.items():
    row = v.copy()
    row["time"] = t
    rows.append(row)

df = pd.DataFrame(rows).sort_values("time")
latest = df.iloc[-1]

# -----------------------------------
# PLOTLY CONFIG
# -----------------------------------
plot_config = {
    "scrollZoom": True,
    "displayModeBar": True,
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d"]
}

# -----------------------------------
# TITLE
# -----------------------------------
st.title("📈 NIFTY LIVE OI DASHBOARD")
st.caption(f"Last Updated: {latest['time']}")

# -----------------------------------
# KPI CARDS
# -----------------------------------
st.subheader("📌 Live KPI Metrics")

r1 = st.columns(5)
r1[0].metric("CI", round(latest["ci"], 2))
r1[1].metric("OI Bias", round(latest["oi_bias"], 2))
r1[2].metric("PCR", round(latest["pcr"], 2))
r1[3].metric("PCR 20", round(latest["pcr_20"], 2))
r1[4].metric("OI Bias 20", round(latest["oi_bias_20"], 2))

r2 = st.columns(4)
r2[0].metric("Support", round(latest["support_sum"], 2))
r2[1].metric("Resistance", round(latest["resistance_sum"], 2))
r2[2].metric("Support 20", round(latest["support_20"], 2))
r2[3].metric("Resistance 20", round(latest["resistance_20"], 2))

r3 = st.columns(2)
r3[0].metric("NIFTY", round(latest["nifty_price"], 2))
r3[1].metric("FUTURE", round(latest["nifty_fut_price"], 2))

# -----------------------------------
# CHARTS
# -----------------------------------
col1, col2 = st.columns(2)

# ---------------- OI BIAS ----------------
with col1:
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["time"],
        y=df["oi_bias"],
        mode="lines+markers",
        name="OI Bias",
        line=dict(color="lime", width=4),
        marker=dict(size=6)
    ))

    fig.add_trace(go.Scatter(
        x=df["time"],
        y=df["oi_bias_20"],
        mode="lines+markers",
        name="OI Bias 20",
        line=dict(color="orange", width=4),
        marker=dict(size=6)
    ))

    fig.add_hrect(y0=15, y1=100, fillcolor="green", opacity=0.08, line_width=0)
    fig.add_hrect(y0=-100, y1=-15, fillcolor="red", opacity=0.08, line_width=0)

    fig.update_layout(
        title="🔥 OI Bias Trend",
        template="plotly_dark",
        height=550,
        dragmode="zoom",
        hovermode="x unified",
        xaxis=dict(
            title="Time",
            rangeslider=dict(visible=True),
            showspikes=True,
            spikemode="across",
            spikesnap="cursor"
        ),
        yaxis=dict(
            title="Bias",
            showspikes=True
        )
    )

    st.plotly_chart(fig, use_container_width=True, config=plot_config)

# ---------------- PCR ----------------
with col2:
    fig2 = go.Figure()

    fig2.add_trace(go.Scatter(
        x=df["time"],
        y=df["pcr"],
        mode="lines+markers",
        name="PCR",
        line=dict(color="deepskyblue", width=4),
        marker=dict(size=6)
    ))

    fig2.add_trace(go.Scatter(
        x=df["time"],
        y=df["pcr_20"],
        mode="lines+markers",
        name="PCR 20",
        line=dict(color="violet", width=4),
        marker=dict(size=6)
    ))

    fig2.add_hline(y=1, line_dash="dash", line_color="white")

    fig2.update_layout(
        title="📊 PCR Trend",
        template="plotly_dark",
        height=550,
        dragmode="zoom",
        hovermode="x unified",
        xaxis=dict(
            title="Time",
            rangeslider=dict(visible=True),
            showspikes=True
        ),
        yaxis=dict(
            title="PCR",
            showspikes=True
        )
    )

    st.plotly_chart(fig2, use_container_width=True, config=plot_config)

# -----------------------------------
# CI GRAPH
# -----------------------------------
st.subheader("⚡ CI Final Graph")

fig3 = go.Figure()

fig3.add_trace(go.Bar(
    x=df["time"],
    y=df["ci"],
    marker_color="cyan",
    name="CI"
))

fig3.update_layout(
    title="⚡ CI Final Graph",
    template="plotly_dark",
    height=450,
    dragmode="zoom",
    hovermode="x unified",
    xaxis=dict(
        title="Time",
        rangeslider=dict(visible=True),
        showspikes=True
    ),
    yaxis=dict(
        title="CI",
        showspikes=True
    )
)

st.plotly_chart(fig3, use_container_width=True, config=plot_config)

# -----------------------------------
# RAW DATA
# -----------------------------------
st.subheader("📋 Raw Data")
st.dataframe(df, use_container_width=True)