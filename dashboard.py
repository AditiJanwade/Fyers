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

# Auto refresh every 30 sec
st_autorefresh(interval=30000, key="live_refresh")

st.markdown("""
<style>
html, body, [class*="css"] {
    font-family: Arial, sans-serif;
}

/* remove top empty gap */
.main .block-container{
    padding-top:0.2rem !important;
    padding-bottom:0rem !important;
    padding-left:1rem !important;
    padding-right:1rem !important;
    max-width:100% !important;
}

/* hide top header gap */
header[data-testid="stHeader"]{
    height:0rem;
}

section.main > div{
    padding-top:0rem !important;
}

/* metric cards */
[data-testid="metric-container"]{
    background:linear-gradient(135deg,#111827,#1f2937);
    border:1px solid #374151;
    padding:6px !important;
    border-radius:10px;
    margin-bottom:4px !important;
}

/* metric label */
[data-testid="metric-container"] label{
    font-size:11px !important;
}

/* metric value */
[data-testid="stMetricValue"]{
    font-size:18px !important;
    line-height:1 !important;
}

/* titles */
h1{
    font-size:28px !important;
    margin-bottom:0rem !important;
}

h2,h3{
    font-size:18px !important;
    margin-bottom:0rem !important;
}

footer{
    visibility:hidden;
}
</style>
""", unsafe_allow_html=True)

# -----------------------------------
# REDIS
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
    st.error("No Data Found")
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
    "displaylogo": False
}

# -----------------------------------
# TITLE
# -----------------------------------
st.markdown("## 📈 NIFTY LIVE OI DASHBOARD")
st.caption(f"Updated: {latest['time']}")

# ==================================================
# TOP SECTION
# LEFT = DATA
# RIGHT = NIFTY CHART
# ==================================================
left, right = st.columns([1, 2])

# -----------------------------------
# LEFT KPI DATA
# -----------------------------------
# -----------------------------------
# LEFT KPI DATA
# Replace your current "with left:" block with this
# -----------------------------------
with left:
    st.subheader("📌 Live Data")

    c1, c2 = st.columns(2)
    c1.metric("CI", round(latest["ci"], 2))
    c2.metric("OI Bias", round(latest["oi_bias"], 2))

    c3, c4 = st.columns(2)
    c3.metric("PCR", round(latest["pcr"], 2))
    c4.metric("OI Bias 20", round(latest["oi_bias_20"], 2))

    c5, c6 = st.columns(2)
    c5.metric("PCR 20", round(latest["pcr_20"], 2))
    c6.metric("NIFTY", round(latest["nifty_price"], 2))

    c7, c8 = st.columns(2)
    c7.metric("FUTURE", round(latest["nifty_fut_price"], 2))
    c8.metric("Support", round(latest["support_sum"], 2))

    c9, c10 = st.columns(2)
    c9.metric("Resistance", round(latest["resistance_sum"], 2))
    c10.metric("Support 20", round(latest["support_20"], 2))

    c11, c12 = st.columns(2)
    c11.metric("Resistance 20", round(latest["resistance_20"], 2))
# -----------------------------------
# RIGHT NIFTY CHART
# -----------------------------------
with right:
    fig_price = go.Figure()

    fig_price.add_trace(go.Scatter(
        x=df["time"],
        y=df["nifty_price"],
        mode="lines+markers",
        name="NIFTY",
        line=dict(color="cyan", width=4),
        marker=dict(size=5),
        hovertemplate="Time: %{x}<br>NIFTY: %{y:.2f}<extra></extra>"
    ))

    fig_price.update_layout(
        title="📈 NIFTY LIVE",
        template="plotly_dark",
        height=360,
        margin=dict(l=10, r=10, t=40, b=10),

        xaxis=dict(
            title="Time",
            showgrid=False,
            tickangle=-35
        ),

        yaxis=dict(
            title="NIFTY",
            showgrid=True,
            tickformat=".0f"   # shows actual values like 24263
        ),

        hovermode="x unified"
    )

    st.plotly_chart(
        fig_price,
        use_container_width=True,
        config=plot_config
    )
# ==================================================
# BOTTOM SECTION
# LEFT = OI BIAS
# RIGHT = PCR
# ==================================================
b1, b2 = st.columns(2)

# -----------------------------------
# OI BIAS CHART
# -----------------------------------
with b1:
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["time"],
        y=df["oi_bias"],
        mode="lines+markers",
        name="OI Bias",
        line=dict(color="lime", width=4)
    ))

    fig.add_trace(go.Scatter(
        x=df["time"],
        y=df["oi_bias_20"],
        mode="lines+markers",
        name="OI Bias 20",
        line=dict(color="orange", width=4)
    ))

    fig.add_hrect(y0=15, y1=100, fillcolor="green", opacity=0.08, line_width=0)
    fig.add_hrect(y0=-100, y1=-15, fillcolor="red", opacity=0.08, line_width=0)

    fig.update_layout(
        title="🔥 OI Bias Trend",
        template="plotly_dark",
        height=380,
        margin=dict(l=10, r=10, t=40, b=10)
    )

    st.plotly_chart(fig, use_container_width=True, config=plot_config)

# -----------------------------------
# PCR CHART
# -----------------------------------
with b2:
    fig2 = go.Figure()

    fig2.add_trace(go.Scatter(
        x=df["time"],
        y=df["pcr"],
        mode="lines+markers",
        name="PCR",
        line=dict(color="deepskyblue", width=4)
    ))

    fig2.add_trace(go.Scatter(
        x=df["time"],
        y=df["pcr_20"],
        mode="lines+markers",
        name="PCR 20",
        line=dict(color="violet", width=4)
    ))

    fig2.add_hline(y=1, line_dash="dash", line_color="white")

    fig2.update_layout(
        title="📊 PCR Trend",
        template="plotly_dark",
        height=380,
        margin=dict(l=10, r=10, t=40, b=10)
    )

    st.plotly_chart(fig2, use_container_width=True, config=plot_config)

# -----------------------------------
# CI GRAPH
# -----------------------------------
st.subheader("⚡ CI Final Graph")

fig3 = go.Figure()

fig3.add_trace(go.Scatter(
    x=df["time"],
    y=df["ci"],
    mode="lines+markers",
    name="CI",
    line=dict(color="cyan", width=4),
    marker=dict(size=5),
    hovertemplate="Time: %{x}<br>CI: %{y:.2f}<extra></extra>"
))

fig3.update_layout(
    title="⚡ CI Final Graph",
    template="plotly_dark",
    height=420,
    margin=dict(l=10, r=10, t=40, b=10),
    hovermode="x unified",

    xaxis=dict(
        title="Time",
        tickangle=-35,
        rangeslider=dict(visible=True)
    ),

    yaxis=dict(
        title="CI",
        showgrid=True
    )
)

st.plotly_chart(fig3, use_container_width=True, config=plot_config)

# -----------------------------------
# RAW DATA
# -----------------------------------
st.subheader("📋 Raw Data")
st.dataframe(df, use_container_width=True)