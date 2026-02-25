# streamlit_app.py

import os
import time
import json
import threading
import queue
from collections import deque
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

# ===== Optional deps =====
try:
    from influxdb_client import InfluxDBClient
    INFLUX_OK = True
except Exception:
    INFLUX_OK = False

try:
    import paho.mqtt.client as mqtt
    MQTT_OK = True
except Exception:
    MQTT_OK = False


# ---------- helpers ----------
def parse_iso(s):
    if not s:
        return None
    if isinstance(s, (int, float)):
        # unix ms or s
        try:
            if float(s) > 2_000_000_000_000:
                return datetime.fromtimestamp(float(s) / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(float(s), tz=timezone.utc)
        except Exception:
            return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def soc_from_voltage(u_v):
    """SOC estimate for 12V branch (returns 0..1).

    Piecewise-linear approximation (dashboard-grade). Under load voltage sags,
    so SOC may fluctuate; apply smoothing if needed.
    """
    if u_v is None or pd.isna(u_v):
        return None

    pts = [
        (11.00, 0.00),
        (11.60, 0.10),
        (11.80, 0.20),
        (12.00, 0.40),
        (12.20, 0.70),
        (12.40, 0.90),
        (12.60, 1.00),
    ]

    if u_v <= pts[0][0]:
        return pts[0][1]
    if u_v >= pts[-1][0]:
        return pts[-1][1]

    for (x1, y1), (x2, y2) in zip(pts[:-1], pts[1:]):
        if x1 <= u_v <= x2:
            t = (u_v - x1) / (x2 - x1)
            return y1 + t * (y2 - y1)
    return None


def estimate_range_km(soc, max_range_km=30.0):
    if soc is None or pd.isna(soc):
        return None
    return float(soc) * float(max_range_km)


def make_line(df, col, title, unit=""):
    fig = go.Figure()
    if col in df.columns and "ts" in df.columns:
        d = df[["ts", col]].dropna()
        if not d.empty:
            fig.add_trace(go.Scatter(x=d["ts"], y=d[col], mode="lines", name=col))
    fig.update_layout(
        title=title,
        xaxis_title="time",
        yaxis_title=unit,
        height=260,
        margin=dict(l=10, r=10, t=40, b=10),
    )
    return fig


# ---------- Demo loader ----------
@st.cache_data(ttl=60)
def load_jsonl(path):
    try:
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                obj["ts"] = parse_iso(obj.get("ts")) or datetime.now(timezone.utc)
                rows.append(obj)
        df = pd.DataFrame(rows)
        if "ts" in df:
            df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            df = df.sort_values("ts")
        return df
    except Exception as e:
        st.error(f"JSONL error: {e}")
        return pd.DataFrame()


# ---------- Influx ----------
@st.cache_data(ttl=30)
def from_influx(url, token, org, bucket, device, window_min):
    if not INFLUX_OK:
        st.error("influxdb-client не установлен.")
        return pd.DataFrame()

    t0 = datetime.now(timezone.utc) - timedelta(minutes=int(window_min))
    start = t0.isoformat()

    try:
        client = InfluxDBClient(url=url, token=token, org=org)
        qapi = client.query_api()
        flux = f'''
from(bucket: "{bucket}")
  |> range(start: {start})
  |> filter(fn: (r) => r._measurement == "scooter")
  |> filter(fn: (r) => r.device_id == "{device}")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time","u_batt_v","i_batt_a","t_batt_c","t_ctrl_c","speed_kmh","ax_ms2","ay_ms2","az_ms2"])
'''
        tables = qapi.query_data_frame(flux)
        if isinstance(tables, list):
            df = pd.concat(tables, ignore_index=True) if tables else pd.DataFrame()
        else:
            df = tables

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.rename(columns={"_time": "ts"})
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df = df.sort_values("ts")
        return df
    except Exception as e:
        st.error(f"Influx error: {e}")
        return pd.DataFrame()


# ---------- MQTT ----------
def start_mqtt_reader(host, port, topic, ignore_synthetic=True):
    q = queue.Queue(maxsize=5000)

    def on_message(_c, _u, msg):
        try:
            payload = msg.payload.decode("utf-8", errors="ignore")
            raw = json.loads(payload)

            # Optional: ignore synthetic emulator stream in LIVE mode
            if ignore_synthetic and raw.get("fw_src") == "synthetic":
                return

            # Accept both:
            # 1) "scooter" schema (emulator / Node-RED / Influx-style)
            # 2) ESP8266 schema: throttle_raw, motor{state,pwm}, wifi{rssi}, temp_c, hall{pulses,delta}
            ts = parse_iso(raw.get("ts")) or datetime.now(timezone.utc)

            if ("throttle_raw" in raw) or ("motor" in raw) or ("wifi" in raw) or ("hall" in raw):
                motor = raw.get("motor") or {}
                wifi = raw.get("wifi") or {}
                hall = raw.get("hall") or {}

                obj = {
                    "ts": ts,

                    # Classic scooter fields (may be None until real sensors are connected)
                    "u_batt_v": raw.get("u_batt_v"),
                    "i_batt_a": raw.get("i_batt_a"),
                    "t_batt_c": raw.get("t_batt_c", raw.get("temp_c")),
                    "t_ctrl_c": raw.get("t_ctrl_c"),

                    # Speed proxy: hall delta -> km/h (placeholder scaling; tune later)
                    "speed_kmh": raw.get("speed_kmh"),
                    "ax_ms2": raw.get("ax_ms2"),
                    "ay_ms2": raw.get("ay_ms2"),
                    "az_ms2": raw.get("az_ms2"),

                    # Extra ESP fields
                    "throttle_raw": raw.get("throttle_raw"),
                    "motor_state": motor.get("state"),
                    "motor_pwm": motor.get("pwm"),
                    "rssi": wifi.get("rssi"),
                    "hall_pulses": hall.get("pulses"),
                    "hall_delta": hall.get("delta"),
                    "fw_src": "esp8266",
                }

                if obj["speed_kmh"] is None:
                    try:
                        obj["speed_kmh"] = float(obj["hall_delta"] or 0.0) * 0.1
                    except Exception:
                        obj["speed_kmh"] = 0.0
            else:
                # Already in scooter schema
                obj = raw
                obj["ts"] = ts
                if "fw_src" not in obj:
                    obj["fw_src"] = "unknown"

            try:
                q.put_nowait(obj)
            except queue.Full:
                # Drop oldest-like behavior: if queue is full, drop this message silently
                pass

        except Exception as e:
            print("MQTT parse error:", e)

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    client.on_message = on_message
    print(f"Attempting MQTT connection to {host}:{port}")
    client.connect(host, int(port), keepalive=30)
    client.subscribe(topic, qos=0)
    print(f"MQTT connected to {host}:{port}, subscribing to {topic}")

    th = threading.Thread(target=client.loop_forever, daemon=True)
    th.start()
    return q


def df_from_queue(q, window_min):
    """
    IMPORTANT FIX:
    - We keep a rolling buffer in st.session_state so the dashboard doesn't show
      "No data" on cycles where no new MQTT messages arrived.
    """
    if "mqtt_buf" not in st.session_state:
        st.session_state["mqtt_buf"] = deque(maxlen=10000)

    buf = st.session_state["mqtt_buf"]

    # Pull everything available now (short timeouts so UI is not blocked)
    while True:
        try:
            obj = q.get(timeout=0.05)
            buf.append(obj)
        except queue.Empty:
            break

    if not buf:
        return pd.DataFrame()

    df = pd.DataFrame(list(buf))
    if "ts" in df:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        t_deadline = datetime.now(timezone.utc) - timedelta(minutes=int(window_min))
        df = df[df["ts"] >= t_deadline].sort_values("ts")
    return df


def get_mqtt_queue(host, port, topic, ignore_synth):
    key = f"{host}:{port}:{topic}:ignore_synth={int(bool(ignore_synth))}"

    # If config changed -> restart reader + reset rolling buffer
    if st.session_state.get("mqtt_cfg") != key or "mqtt_q" not in st.session_state:
        st.session_state["mqtt_cfg"] = key
        st.session_state["mqtt_q"] = start_mqtt_reader(host, port, topic, ignore_synthetic=ignore_synth)
        st.session_state["mqtt_buf"] = deque(maxlen=10000)

    return st.session_state["mqtt_q"]


# ---------- UI ----------
st.title("M365 Digital Twin Dashboard")

st.sidebar.header("Настройки")
mode = st.sidebar.radio("Режим работы", ["Demo (JSONL)", "Live (InfluxDB)", "Live (MQTT)"])
refresh_sec = st.sidebar.slider("Период автообновления (сек.)", 1, 30, 5)
window_min = st.sidebar.slider("Окно данных (мин.)", 1, 120, 15)
speed_med_n = st.sidebar.slider("Медианный фильтр скорости (N)", 1, 15, 5)
speed_max_kmh = st.sidebar.slider("Speed clamp max (km/h)", 5, 80, 50)
speed_ema_alpha = st.sidebar.slider("Speed EMA alpha", 0.05, 0.60, 0.25)



# Zero-lock controls (fast drop to zero when motor stops / no pulses)
pwm_zero_lock = st.sidebar.slider("PWM zero-lock", 0, 300, 140)
stop_hold_sec = st.sidebar.slider("Stop hold (sec)", 0.0, 2.0, 0.8)
if mode == "Demo (JSONL)":
    path = st.sidebar.text_input("Путь к JSONL", value="logs/raw/2025-09-26_m365_synthetic.jsonl")

elif mode == "Live (InfluxDB)":
    url = st.sidebar.text_input("Influx URL", "http://localhost:8086")
    token = st.sidebar.text_input("Token", "", type="password")
    org = st.sidebar.text_input("Org", "my-org")
    bucket = st.sidebar.text_input("Bucket", "scooter")
    device = st.sidebar.text_input("Device ID", "m365-lis-01")

else:
    mqtt_host = st.sidebar.text_input("MQTT host", "localhost")
    mqtt_port = st.sidebar.number_input("MQTT port", 1, 65535, 1883)
    mqtt_topic = st.sidebar.text_input("Topic filter", "m365/#")

    # Optional: ignore synthetic stream when you're watching ESP
    ignore_synth = st.sidebar.checkbox("Игнорировать synthetic (эмулятор)", value=True)

# Autorefresh only in Live modes
if mode.startswith("Live"):
    st_autorefresh(interval=refresh_sec * 1000, key="live_autorefresh")

    # Clear cache ONLY for Influx (MQTT isn't cached and shouldn't be cleared)
    if mode == "Live (InfluxDB)":
        st.cache_data.clear()

# ---------- Data source ----------
if mode == "Demo (JSONL)":
    df = load_jsonl(path)

elif mode == "Live (InfluxDB)":
    df = from_influx(url, token, org, bucket, device, window_min)

else:
    if not MQTT_OK:
        st.error("paho-mqtt не установлен.")
        st.stop()
    q = get_mqtt_queue(mqtt_host, mqtt_port, mqtt_topic, ignore_synth)
    df = df_from_queue(q, window_min)

# ---------- Empty handling ----------
if df is None or df.empty:
    st.info("Нет данных для отображения. Проверь источник и фильтры.")
    st.stop()

# ---------- metrics ----------
# Ensure the expected columns exist (so the dashboard doesn't crash on partial schemas)
for col in ["u_batt_v", "i_batt_a", "t_batt_c", "t_ctrl_c", "speed_kmh", "ax_ms2", "ay_ms2", "az_ms2"]:
    if col not in df.columns:
        df[col] = None

# --- Speed filtering (Hall noise tolerant): clamp -> median -> robust outlier reject -> EMA ---
speed_raw = pd.to_numeric(df["speed_kmh"], errors="coerce")

# A) Physical clamp
speed_clamped = speed_raw.clip(lower=0, upper=float(speed_max_kmh))

# B) Rolling median
speed_med = speed_clamped.rolling(window=int(speed_med_n), min_periods=1).median()

# C) Robust outlier rejection (MAD-based, Hampel-like)
mad = (speed_clamped - speed_med).abs().rolling(window=int(speed_med_n), min_periods=1).median()
robust_sigma = 1.4826 * mad
# if sigma is 0 (flat line), avoid marking everything as outlier
threshold = (4.0 * robust_sigma).where(robust_sigma > 0, other=0.0)
is_outlier = (speed_clamped - speed_med).abs() > threshold

speed_no_out = speed_clamped.where(~is_outlier, speed_med)

# D) Exponential moving average (instrument-like smoothing)
df["speed_kmh_filt"] = speed_no_out.ewm(alpha=float(speed_ema_alpha), adjust=False).mean()



# --- Zero-lock: force speed to 0 quickly when motor is stopped or pulses cease ---
# Uses available ESP/MQTT fields if present: motor_state, motor_pwm, hall_delta
if "ts" in df.columns:
    motor_pwm = pd.to_numeric(df["motor_pwm"], errors="coerce") if "motor_pwm" in df.columns else pd.Series([pd.NA] * len(df))
    motor_state = df["motor_state"] if "motor_state" in df.columns else pd.Series([pd.NA] * len(df))
    hall_delta = pd.to_numeric(df["hall_delta"], errors="coerce") if "hall_delta" in df.columns else pd.Series([pd.NA] * len(df))

    is_motor_stop = motor_state.astype(str).str.upper().eq("STOP")
    is_pwm_low = motor_pwm.notna() & (motor_pwm <= float(pwm_zero_lock))

    # time since last pulse (based on hall_delta == 0)
    dt = df["ts"].diff().dt.total_seconds().fillna(0.0)
    no_pulse = hall_delta.fillna(0).astype(float) <= 0.0

    # Accumulate time only while no_pulse holds, reset on any pulse
    t_since_pulse = (dt.where(no_pulse, 0.0)).groupby((~no_pulse).cumsum()).cumsum()
    is_no_pulse_long = t_since_pulse >= float(stop_hold_sec)

    force_zero = is_motor_stop | is_pwm_low | is_no_pulse_long

    # Force zero and (optionally) also clamp tiny residuals
    df.loc[force_zero, "speed_kmh_filt"] = 0.0
    df["speed_kmh_filt"] = pd.to_numeric(df["speed_kmh_filt"], errors="coerce").fillna(0.0)
df["soc"] = df["u_batt_v"].apply(soc_from_voltage)
df["range_km"] = df["soc"].apply(lambda s: estimate_range_km(s, 30.0))

last = df.tail(1).iloc[0]


def fmt_num(v, d=2):
    import math
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    return f"{float(v):.{d}f}"


col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("U batt (V)", fmt_num(last.get("u_batt_v")))
col2.metric("I batt (A)", fmt_num(last.get("i_batt_a")))
col3.metric("Speed (km/h)", fmt_num(last.get("speed_kmh_filt")))
col4.metric("T batt (°C)", fmt_num(last.get("t_batt_c")))
col5.metric("SOC", fmt_num(last.get("soc"), 2))

# Extra ESP-only fields (shown when present)
extra_cols = ["throttle_raw", "motor_state", "motor_pwm", "rssi", "hall_pulses", "hall_delta", "fw_src"]
have_extra = any(c in df.columns and df[c].notna().any() for c in extra_cols)
if have_extra:
    st.markdown("### ESP / MQTT поля")
    e1, e2, e3, e4, e5, e6 = st.columns(6)
    e1.metric("Throttle raw", str(last.get("throttle_raw", "—")))
    e2.metric("Motor state", str(last.get("motor_state", "—")))
    e3.metric("Motor pwm", str(last.get("motor_pwm", "—")))
    e4.metric("RSSI", str(last.get("rssi", "—")))
    e5.metric("Hall Δ", str(last.get("hall_delta", "—")))
    e6.metric("FW src", str(last.get("fw_src", "—")))
else:
    # Still show fw_src if present in scooter schema
    if "fw_src" in df.columns and df["fw_src"].notna().any():
        st.caption(f"FW src: {last.get('fw_src')}")

# ---------- charts ----------
c1, c2 = st.columns(2)
c1.plotly_chart(make_line(df, "u_batt_v", "Battery Voltage", "V"), use_container_width=True)
c2.plotly_chart(make_line(df, "i_batt_a", "Battery Current", "A"), use_container_width=True)

c3, c4 = st.columns(2)
c3.plotly_chart(make_line(df, "speed_kmh_filt", "Speed", "km/h"), use_container_width=True)
c4.plotly_chart(make_line(df, "t_batt_c", "Battery Temp", "°C"), use_container_width=True)

st.plotly_chart(make_line(df, "t_ctrl_c", "Controller Temp", "°C"), use_container_width=True)
st.plotly_chart(make_line(df, "soc", "SOC (0..1)"), use_container_width=True)

st.markdown("### Последние сообщения")
st.dataframe(df.tail(50), use_container_width=True)
