import os
import time
import json
import pathlib
import math
from datetime import datetime, timedelta, timezone

import streamlit as st
import pandas as pd

# Optional: Plotly for interactive charts
import plotly.express as px
import plotly.graph_objects as go

# Try to import InfluxDB client if available
INFLUX_OK = True
try:
    from influxdb_client import InfluxDBClient, Point
    from influxdb_client.client.write_api import SYNCHRONOUS
except Exception:
    INFLUX_OK = False

st.set_page_config(page_title="M365 Digital Twin Dashboard", layout="wide")

# --------------------------------------
# Helpers
# --------------------------------------
def parse_iso(ts):
    try:
        if ts.endswith("Z"):
            return datetime.fromisoformat(ts.replace("Z","+00:00"))
        return datetime.fromisoformat(ts)
    except Exception:
        return None

def soc_from_voltage(u_v: float) -> float:
    # Crude linear approximation for 10s Li-ion pack (42.0 V full, 33.0 V empty)
    if u_v is None: 
        return None
    soc = (u_v - 33.0) / (42.0 - 33.0)
    return float(max(0.0, min(1.0, soc)))

def estimate_range_km(soc: float, nominal_km: float = 30.0) -> float:
    if soc is None:
        return None
    return float(max(0.0, nominal_km * soc))

def load_jsonl(path: str) -> pd.DataFrame:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if "marker" in obj:
                # marker row
                rows.append({
                    "ts": parse_iso(obj.get("ts")),
                    "marker": obj.get("marker")
                })
            else:
                rows.append({
                    "ts": parse_iso(obj.get("ts")),
                    "u_batt_v": obj.get("u_batt_v"),
                    "i_batt_a": obj.get("i_batt_a"),
                    "t_batt_c": obj.get("t_batt_c"),
                    "t_ctrl_c": obj.get("t_ctrl_c"),
                    "speed_kmh": obj.get("speed_kmh"),
                    "ax_ms2": obj.get("ax_ms2"),
                    "ay_ms2": obj.get("ay_ms2"),
                    "az_ms2": obj.get("az_ms2"),
                    "fw_src": obj.get("fw_src","synthetic"),
                })
    df = pd.DataFrame(rows)
    df = df.sort_values("ts")
    return df

def query_influx(url, token, org, bucket, measurement="scooter", device_id="m365-lis-01", minutes=30):
    if not INFLUX_OK:
        st.warning("InfluxDB client не установлен. Установи пакет influxdb-client, чтобы использовать Live-режим.")
        return pd.DataFrame()
    client = InfluxDBClient(url=url, token=token, org=org, timeout=30_000)
    query_api = client.query_api()
    start = f"-{minutes}m"
    flux = f'''
from(bucket:"{bucket}")
  |> range(start: {start})
  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
  |> filter(fn: (r) => r["device_id"] == "{device_id}")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","u_batt_v","i_batt_a","t_batt_c","t_ctrl_c","speed_kmh","ax_ms2","ay_ms2","az_ms2","fw_src"])
'''
    tables = query_api.query_data_frame(flux)
    if isinstance(tables, list) and len(tables)==0:
        return pd.DataFrame()
    if isinstance(tables, list):
        df = pd.concat(tables, ignore_index=True)
    else:
        df = tables
    if df.empty:
        return df
    if "_time" in df.columns:
        df.rename(columns={"_time":"ts"}, inplace=True)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts")
    return df

# --------------------------------------
# Sidebar
# --------------------------------------
st.sidebar.header("Настройки")
mode = st.sidebar.radio("Режим работы", ["Demo (JSONL)", "Live (InfluxDB)"])

if mode == "Demo (JSONL)":
    default_path = "logs/raw/2025-09-26_m365_synthetic.jsonl"
    path = st.sidebar.text_input("Путь к JSONL", value=default_path)
else:
    url = st.sidebar.text_input("Influx URL", value=os.getenv("INFLUX_URL","http://localhost:8086"))
    token = st.sidebar.text_input("Influx Token", value=os.getenv("INFLUX_TOKEN",""), type="password")
    org = st.sidebar.text_input("Influx Org", value=os.getenv("INFLUX_ORG","my-org"))
    bucket = st.sidebar.text_input("Influx Bucket", value=os.getenv("INFLUX_BUCKET","scooter"))
    device = st.sidebar.text_input("Device ID", value="m365-lis-01")
refresh_sec = st.sidebar.slider("Обновление (сек.)", min_value=2, max_value=30, value=5, step=1)
window_min = st.sidebar.slider("Окно, минут", min_value=5, max_value=120, value=30, step=5)

st.sidebar.markdown("---")
st.sidebar.caption("M365 Digital Twin • Streamlit Dashboard")

# --------------------------------------
# Data load
# --------------------------------------
@st.cache_data(ttl=5)
def get_demo_df(path, window_min):
    df = load_jsonl(path)
    if df.empty:
        return df
    tmax = df["ts"].max()
    tmin = tmax - timedelta(minutes=window_min)
    return df[df["ts"].between(tmin, tmax)]

@st.cache_data(ttl=5)
def get_live_df(url, token, org, bucket, device, window_min):
    df = query_influx(url, token, org, bucket, device_id=device, minutes=window_min)
    return df

placeholder = st.empty()

def render(df):
    if df is None or df.empty:
        st.warning("Нет данных для отображения.")
        return

    latest = df.dropna(subset=["u_batt_v","i_batt_a","speed_kmh","t_batt_c","t_ctrl_c"]).iloc[-1] if not df.dropna().empty else None
    u = float(latest["u_batt_v"]) if latest is not None else None
    i = float(latest["i_batt_a"]) if latest is not None else None
    v_kmh = float(latest["speed_kmh"]) if latest is not None else None
    t_b = float(latest["t_batt_c"]) if latest is not None else None
    t_c = float(latest["t_ctrl_c"]) if latest is not None else None
    soc = soc_from_voltage(u) if u is not None else None
    rng = estimate_range_km(soc) if soc is not None else None
    pwr = (u*i) if (u is not None and i is not None) else None

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("U батареи (В)", f"{u:.2f}" if u is not None else "—")
    c2.metric("I батареи (А)", f"{i:.2f}" if i is not None else "—")
    c3.metric("Мощность (Вт)", f"{pwr:.0f}" if pwr is not None else "—")
    c4.metric("Скорость (км/ч)", f"{v_kmh:.1f}" if v_kmh is not None else "—")
    c5.metric("SOC (оценка)", f"{soc*100:,.0f}%" if soc is not None else "—")
    c6.metric("Запас хода (км)", f"{rng:.1f}" if rng is not None else "—")

    # Charts
    with st.container():
        left, right = st.columns(2)
        with left:
            fig = go.Figure()
            if "u_batt_v" in df.columns:
                fig.add_trace(go.Scatter(x=df["ts"], y=df["u_batt_v"], mode="lines", name="U_batt (V)"))
            if "i_batt_a" in df.columns:
                fig.add_trace(go.Scatter(x=df["ts"], y=df["i_batt_a"], mode="lines", name="I_batt (A)", yaxis="y2"))
            fig.update_layout(
                title="Напряжение и ток",
                xaxis_title="Время",
                yaxis_title="Вольты",
                yaxis2=dict(title="Амперы", overlaying="y", side="right", showgrid=False),
                height=350,
            )
            st.plotly_chart(fig, use_container_width=True)

            fig2 = go.Figure()
            if "t_batt_c" in df.columns:
                fig2.add_trace(go.Scatter(x=df["ts"], y=df["t_batt_c"], mode="lines", name="T_batt (°C)"))
            if "t_ctrl_c" in df.columns:
                fig2.add_trace(go.Scatter(x=df["ts"], y=df["t_ctrl_c"], mode="lines", name="T_ctrl (°C)"))
            fig2.update_layout(title="Температуры", xaxis_title="Время", yaxis_title="°C", height=350)
            st.plotly_chart(fig2, use_container_width=True)
        with right:
            if "speed_kmh" in df.columns:
                fig3 = px.area(df, x="ts", y="speed_kmh", title="Скорость (км/ч)")
                st.plotly_chart(fig3, use_container_width=True)
            if "u_batt_v" in df.columns and "i_batt_a" in df.columns:
                dfp = df.copy()
                dfp["pwr_w"] = dfp["u_batt_v"] * dfp["i_batt_a"]
                fig4 = px.area(dfp, x="ts", y="pwr_w", title="Мощность (Вт)")
                st.plotly_chart(fig4, use_container_width=True)

    # Raw preview
    with st.expander("Показать сырые данные"):
        st.dataframe(df.tail(200), use_container_width=True)

# --------------------------------------
# Main loop (manual refresh)
# --------------------------------------
tab1, tab2 = st.tabs(["Дэшборд", "Справка"])

with tab1:
    if mode == "Demo (JSONL)":
        df = get_demo_df(path, window_min)
    else:
        df = get_live_df(url, token, org, bucket, device, window_min)
    render(df)
    st.caption("Ручное обновление: нажми кнопку ниже.")
    if st.button("Обновить данные"):
        st.rerun()

with tab2:
    st.markdown('''
**M365 Digital Twin Dashboard** — локальное приложение для наглядного мониторинга телеметрии самоката Xiaomi M365.

**Режимы:**
- *Demo (JSONL)* — читает файл `jsonl` из папки `logs/raw`, подходит для демонстрации.
- *Live (InfluxDB)* — тянет последние N минут из InfluxDB.

**Метрики:**
- SOC рассчитывается по грубой линейной аппроксимации напряжения (42.0V → 100%, 33.0V → 0%). На этапах 4–5 можно заменить на кулон-каунтинг + OCV.
- Запас хода оценивается как SOC × 30 км (номинальный пробег M365).

Источник проекта: учебный прототип ЦД.
''')

# Auto-refresh disabled to prevent blinking; use manual button instead
