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
header{
    visibility:hidden;
}
/* remove top empty gap */
.main .block-container{
    padding-top:0 !important;
    margin-top:-30px !important;
    padding-bottom:0rem !important;
    padding-left:0 !important;
    padding-right:0 !important;
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


# ==================================================
# TOP SECTION
# LEFT = DATA
# RIGHT = NIFTY CHART
# ==================================================
left, right = st.columns([1,1.5])

# -----------------------------------
# LEFT KPI DATA
# -----------------------------------
# -----------------------------------
# LEFT KPI DATA
# Replace your current "with left:" block with this
# -----------------------------------
# -----------------------------------
# LIVE DATA COLORS
# Add this helper function ABOVE: with left:
# -----------------------------------
def color_metric(label, value, color):
    html = f"""
    <div style="margin-bottom:10px; padding:8px; background:#111827; border:1px solid #374151; border-radius:10px;">
        <div style="font-size:12px; font-weight:700; color:{color}; margin-bottom:4px;">
            {label}
        </div>
        <div style="font-size:22px; font-weight:800; color:{color};">
            {value}
        </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)
with left:
    st.markdown("## 📈 NIFTY LIVE OI DASHBOARD")
    st.caption(f"Updated: {latest['time']}")
    st.subheader("📌 Live Data")

    c1, c2,c3 = st.columns(3)
    c1.metric("NIFTY", round(latest["nifty_price"], 2))
    c2.metric("FUTURE", round(latest["nifty_fut_price"], 2))
    c3.metric("CI", round(latest["ci"], 2))

    c4, c5 = st.columns(2)
    c4.metric("OI Bias", round(latest["oi_bias"], 2))
    c5.metric("OI Bias 20", round(latest["oi_bias_20"], 2))

    c6, c7 = st.columns(2)
    c6.metric("PCR", round(latest["pcr"], 2))
    c7.metric("PCR 20", round(latest["pcr_20"], 2))

    c8, c9 = st.columns(2)

    with c8:
        color_metric("Support", round(latest["support_sum"], 2), "#a78bfa")   # violet

    with c9:
        color_metric("Resistance", round(latest["resistance_sum"], 2), "#fb923c")   # orange


    c10, c11 = st.columns(2)

    with c10:
        color_metric("Support 20", round(latest["support_20"], 2), "#c4b5fd")   # lavender

    with c11:
        color_metric("Resistance 20", round(latest["resistance_20"], 2), "#fdba74")   # peach

    
    
# -----------------------------------
# RIGHT NIFTY CHART
# -----------------------------------
with right:

    fig_price = go.Figure()

    # NIFTY
    fig_price.add_trace(go.Scatter(
        x=df["time"],
        y=df["nifty_price"],
        mode="lines",
        name="NIFTY",
        line=dict(color="cyan", width=4)
    ))

    # FUTURE (only valid rows)
    future_df = df[df["nifty_fut_price"] > 0]

    fig_price.add_trace(go.Scatter(
        x=future_df["time"],
        y=future_df["nifty_fut_price"],
        mode="lines",
        name="FUTURE",
        line=dict(color="orange", width=2, dash="dot"),
        opacity=0.8
    ))

    # Manual scale preserve
    ymin = min(df["nifty_price"].min(), future_df["nifty_fut_price"].min()) - 20
    ymax = max(df["nifty_price"].max(), future_df["nifty_fut_price"].max()) + 20

    fig_price.update_layout(
        title={"text":"📈 NIFTY LIVE", "font":{"color":"cyan","size":22}},
        template="plotly_dark",
        height=430,
        margin=dict(l=10,r=10,t=70,b=10),

        yaxis=dict(
            title="NIFTY",
            range=[ymin, ymax],
            tickformat=".0f"
        ),

        xaxis=dict(
            title="Time",
            tickangle=-35
        ),

        hovermode="x unified"
    )

    st.plotly_chart(fig_price, use_container_width=True, config=plot_config)
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
        title={
        "text": "🔥 OI Bias Trend",
        "font": {"color": "#facc15", "size": 22}   # neutral gold
    },
        template="plotly_dark",
        height=430,
        margin=dict(l=10, r=10, t=70, b=10)
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
        title={
        "text": "📊 PCR Trend",
        "font": {"color": "#d946ef", "size": 22}   # purple
    },
        template="plotly_dark",
        height=430,
        margin=dict(l=10, r=10, t=70, b=10)
    )

    st.plotly_chart(fig2, use_container_width=True, config=plot_config)

# ==========================================
# SUPPORT / RESISTANCE CHARTS
# ==========================================
s1, s2 = st.columns(2)

# -----------------------------
# Support vs Resistance
# -----------------------------
with s1:
    fig_sr = go.Figure()

    fig_sr.add_trace(go.Scatter(
        x=df["time"],
        y=df["support_sum"],
        mode="lines+markers",
        name="Support",
        line=dict(color="#a78bfa", width=3)
    ))

    fig_sr.add_trace(go.Scatter(
        x=df["time"],
        y=df["resistance_sum"],
        mode="lines+markers",
        name="Resistance",
        line=dict(color="#fb923c", width=3)
    ))

    fig_sr.update_layout(
    title={
        "text":"📦 Support vs Resistance",
        "font":{"color":"#00E5FF","size":20}
    },
    template="plotly_dark",
    height=380,
    margin=dict(l=10,r=10,t=70,b=10),
    hovermode="x unified",

    xaxis=dict(
        title="Time",
        color="#a78bfa"
    ),

    yaxis=dict(
        title="OI Value",
        color="#fb923c"
    ),

    legend=dict(
        font=dict(color="#ffffff")
    )
)

    st.plotly_chart(fig_sr, use_container_width=True, config=plot_config)


# -----------------------------
# Support20 vs Resistance20
# -----------------------------
with s2:
    fig_sr20 = go.Figure()

    fig_sr20.add_trace(go.Scatter(
        x=df["time"],
        y=df["support_20"],
        mode="lines+markers",
        name="Support 20",
        line=dict(color="#c4b5fd", width=3)
    ))

    fig_sr20.add_trace(go.Scatter(
        x=df["time"],
        y=df["resistance_20"],
        mode="lines+markers",
        name="Resistance 20",
        line=dict(color="#fdba74", width=3)
    ))

    fig_sr20.update_layout(
    title={
        "text":"📊 Support20 vs Resistance20",
        "font":{"color":"#38BDF8","size":20}
    },
    template="plotly_dark",
    height=380,
    margin=dict(l=10,r=10,t=70,b=10),
    hovermode="x unified",

    xaxis=dict(
        title="Time",
        color="#c4b5fd"
    ),

    yaxis=dict(
        title="OI Value",
        color="#fdba74"
    ),

    legend=dict(
        font=dict(color="#ffffff")
    )
)

    st.plotly_chart(fig_sr20, use_container_width=True, config=plot_config)
# -----------------------------------
# CI GRAPH
# -----------------------------------


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
     title={
        "text": "⚡ CI Final Graph",
        "font": {"color": "#38bdf8", "size": 22}   # sky blue
    },
    template="plotly_dark",
    height=420,
    margin=dict(l=10, r=10, t=70, b=10),
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