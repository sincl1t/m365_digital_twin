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
import streamlit.components.v1 as components

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

def make_gauge(value, title, vmin, vmax, unit="", thresholds=None):
    if value is None or pd.isna(value):
        value = 0

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=float(value),
        number={'suffix': f" {unit}"},
        title={'text': title},
        gauge={
            'axis': {'range': [vmin, vmax]},
            'bar': {'thickness': 0.3},
            'steps': thresholds if thresholds else [],
        }
    ))

    fig.update_layout(height=260, margin=dict(l=10, r=10, t=40, b=10))
    return fig

from PIL import Image
import numpy as np

def colorize_scooter(img_path, color):

    img = Image.open(img_path).convert("RGBA")
    data = np.array(img)

    r,g,b = {
        "green": (0,200,0),
        "yellow": (240,200,0),
        "red": (220,0,0)
    }[color]

    # определяем белый фон
    white_mask = (data[:,:,0] > 240) & (data[:,:,1] > 240) & (data[:,:,2] > 240)

    # делаем его прозрачным
    data[white_mask] = [255,255,255,0]

    # перекрашиваем самокат
    scooter_mask = ~white_mask
    data[scooter_mask] = [r,g,b,255]

    return Image.fromarray(data)

def render_motion_bar(speed, max_speed=50):
    try:
        speed = float(speed) if speed is not None and not pd.isna(speed) else 0.0
    except Exception:
        speed = 0.0

    speed = max(0.0, min(speed, float(max_speed)))
    pct = speed / float(max_speed)

    if speed < 15:
        glow = "#7CFC00"
    elif speed < 30:
        glow = "#FFD700"
    else:
        glow = "#FF4D4D"

    left_pct = 4 + pct * 88
    fill_pct = pct * 92

    return f"""
    <html>
    <body style="margin:0; background:transparent; font-family:Arial, sans-serif;">
        <div style="
            width:100%;
            height:110px;
            position:relative;
            border-radius:20px;
            background:rgba(255,255,255,0.03);
            border:1px solid rgba(255,255,255,0.08);
            overflow:hidden;
        ">
            <div style="
                position:absolute;
                left:4%;
                top:68px;
                width:92%;
                height:8px;
                border-radius:999px;
                background:rgba(255,255,255,0.10);
            "></div>

            <div style="
                position:absolute;
                left:4%;
                top:68px;
                width:{fill_pct:.1f}%;
                height:8px;
                border-radius:999px;
                background:{glow};
                box-shadow:0 0 12px {glow};
            "></div>

            <div style="
                position:absolute;
                left:{left_pct:.1f}%;
                top:18px;
                transform:translateX(-50%);
                font-size:42px;
            ">🛴</div>

            <div style="
                position:absolute;
                right:16px;
                top:14px;
                color:white;
                font-size:18px;
                font-weight:700;
            ">{speed:.1f} км/ч</div>

            <div style="
                position:absolute;
                left:16px;
                bottom:10px;
                color:rgba(255,255,255,0.6);
                font-size:13px;
            ">Старт</div>

            <div style="
                position:absolute;
                right:16px;
                bottom:10px;
                color:rgba(255,255,255,0.6);
                font-size:13px;
            ">Макс. скорость</div>
        </div>
    </body>
    </html>
    """
    return html

def scooter_health(last):
    score = 0
    issues = []

    t = last.get("t_batt_c")
    u = last.get("u_batt_v")
    rssi = last.get("rssi")
    motor = str(last.get("motor_state", "")).upper()

    if t is not None:
        if t < 50:
            score += 1
        elif t < 65:
            score += 0.5
            issues.append("повышенная температура батареи")
        else:
            issues.append("критическая температура батареи")

    if u is not None:
        if u > 12.2:
            score += 1
        elif u > 11.5:
            score += 0.5
            issues.append("пониженное напряжение батареи")
        else:
            issues.append("критически низкое напряжение")

    if rssi is not None:
        if rssi > -70:
            score += 1
        elif rssi > -85:
            score += 0.5
            issues.append("слабый сигнал связи")
        else:
            issues.append("очень слабый сигнал связи")

    if motor == "STOP":
        issues.append("мотор остановлен")

    if score >= 2.5:
        status = "good"
        color = "green"
    elif score >= 1.5:
        status = "warning"
        color = "yellow"
    else:
        status = "bad"
        color = "red"

    return status, color, issues

def safe_float(v):
    try:
        if v is None or pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None


def describe_level(value, good_cond, warn_cond, good_text, warn_text, bad_text):
    if value is None:
        return "данные отсутствуют"
    if good_cond(value):
        return good_text
    if warn_cond(value):
        return warn_text
    return bad_text


def build_detailed_report(df, last):
    lines = []

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"Время анализа: {now_str}")

    speed = safe_float(last.get("speed_kmh_filt"))
    u_batt = safe_float(last.get("u_batt_v"))
    i_batt = safe_float(last.get("i_batt_a"))
    t_batt = safe_float(last.get("t_batt_c"))
    soc = safe_float((last.get("soc") or 0) * 100)
    rssi = safe_float(last.get("rssi"))
    motor_state = str(last.get("motor_state", "—"))

    lines.append("")
    lines.append("Общая оценка состояния:")
    if t_batt is not None and t_batt < 50 and (u_batt is not None and u_batt >= 11.5):
        lines.append("Состояние самоката оценивается как стабильное. Критических отклонений по основным параметрам не выявлено.")
    elif t_batt is not None and t_batt < 65:
        lines.append("Состояние самоката в целом работоспособное, однако присутствуют параметры, требующие наблюдения.")
    else:
        lines.append("Состояние самоката нестабильное. Обнаружены признаки, указывающие на необходимость диагностики.")

    lines.append("")
    lines.append("Анализ аккумуляторной системы:")
    if u_batt is None:
        lines.append("Напряжение батареи отсутствует в телеметрии, поэтому корректная оценка уровня питания ограничена.")
    elif u_batt < 11.5:
        lines.append(f"Напряжение батареи составляет {u_batt:.2f} В, что соответствует пониженному уровню и может указывать на разряд либо некорректность измерительного канала.")
    elif u_batt < 12.2:
        lines.append(f"Напряжение батареи составляет {u_batt:.2f} В. Показатель находится в допустимой зоне, но ниже оптимального уровня.")
    else:
        lines.append(f"Напряжение батареи составляет {u_batt:.2f} В и находится в нормальном диапазоне.")

    if soc is not None:
        if soc < 20:
            lines.append(f"Расчётный уровень заряда оценивается в {soc:.1f} %, что соответствует низкому запасу энергии.")
        elif soc < 60:
            lines.append(f"Расчётный уровень заряда оценивается в {soc:.1f} %, что соответствует среднему запасу энергии.")
        else:
            lines.append(f"Расчётный уровень заряда оценивается в {soc:.1f} %, что соответствует комфортному запасу энергии.")

    if i_batt is None:
        lines.append("Данные по току батареи отсутствуют.")
    else:
        lines.append(f"Текущий ток батареи составляет {i_batt:.2f} А.")

    if t_batt is None:
        lines.append("Температура батареи отсутствует в потоке данных.")
    elif t_batt < 50:
        lines.append(f"Температура батареи равна {t_batt:.2f} °C и не указывает на тепловую перегрузку.")
    elif t_batt < 65:
        lines.append(f"Температура батареи равна {t_batt:.2f} °C. Наблюдается повышенный нагрев, рекомендуется контроль при дальнейшей эксплуатации.")
    else:
        lines.append(f"Температура батареи равна {t_batt:.2f} °C и находится в опасной зоне.")

    lines.append("")
    lines.append("Анализ движения и привода:")
    if speed is None:
        lines.append("Скорость не определена.")
    elif speed <= 0.5:
        lines.append("В текущий момент самокат находится в состоянии покоя.")
    elif speed < 15:
        lines.append(f"Текущая скорость составляет {speed:.2f} км/ч, что соответствует спокойному режиму движения.")
    elif speed < 30:
        lines.append(f"Текущая скорость составляет {speed:.2f} км/ч, что соответствует штатному эксплуатационному режиму.")
    else:
        lines.append(f"Текущая скорость составляет {speed:.2f} км/ч. Наблюдается повышенная скорость движения.")

    lines.append(f"Состояние мотора: {motor_state}.")
    if motor_state.upper() == "STOP" and speed is not None and speed <= 0.5:
        lines.append("Состояние привода согласуется с отсутствием движения.")
    elif motor_state.upper() == "STOP" and speed is not None and speed > 0.5:
        lines.append("Обнаружено расхождение: мотор отмечен как STOP, однако скорость ненулевая. Рекомендуется проверить логику телеметрии.")
    elif motor_state.upper() not in ["—", "NONE", "NAN", ""]:
        lines.append("Привод передаёт рабочий статус без явных противоречий.")

    lines.append("")
    lines.append("Анализ связи и качества телеметрии:")
    if rssi is None:
        lines.append("Данные по уровню сигнала отсутствуют.")
    elif rssi > -70:
        lines.append(f"Уровень сигнала RSSI составляет {rssi:.0f} dBm, качество связи хорошее.")
    elif rssi > -85:
        lines.append(f"Уровень сигнала RSSI составляет {rssi:.0f} dBm, связь допустимая, но возможны кратковременные потери устойчивости.")
    else:
        lines.append(f"Уровень сигнала RSSI составляет {rssi:.0f} dBm, качество связи низкое.")

    if len(df) >= 5:
        recent = df.tail(min(20, len(df))).copy()

        vb = pd.to_numeric(recent.get("u_batt_v"), errors="coerce")
        tb = pd.to_numeric(recent.get("t_batt_c"), errors="coerce")
        sp = pd.to_numeric(recent.get("speed_kmh_filt"), errors="coerce")

        if vb.notna().sum() >= 3:
            dv = vb.iloc[-1] - vb.iloc[0]
            if dv < -0.2:
                lines.append("За последнее окно наблюдения заметно снижение напряжения батареи.")
            elif dv > 0.2:
                lines.append("За последнее окно наблюдения напряжение батареи возросло, что может быть связано с изменением нагрузки или нестабильностью измерения.")
            else:
                lines.append("Напряжение батареи в пределах окна наблюдения оставалось относительно стабильным.")

        if tb.notna().sum() >= 3:
            dt = tb.iloc[-1] - tb.iloc[0]
            if dt > 1.0:
                lines.append("Температура батареи демонстрирует выраженную тенденцию к росту.")
            elif dt > 0.2:
                lines.append("Температура батареи постепенно увеличивается.")
            else:
                lines.append("Температура батареи за окно наблюдения существенно не изменилась.")

        if sp.notna().sum() >= 3:
            smax = sp.max()
            if smax <= 0.5:
                lines.append("В пределах окна наблюдения движение практически отсутствовало.")
            else:
                lines.append(f"Максимальная зафиксированная скорость в текущем окне составила {smax:.2f} км/ч.")

    lines.append("")
    lines.append("Рекомендации:")
    recs = []

    if u_batt is not None and u_batt < 11.5:
        recs.append("проверить источник питания, цепь измерения напряжения и фактический уровень заряда")
    if t_batt is not None and t_batt >= 50:
        recs.append("проконтролировать температурный режим батареи при дальнейшей работе")
    if rssi is not None and rssi <= -85:
        recs.append("улучшить качество беспроводного соединения или уменьшить расстояние до точки доступа")
    if motor_state.upper() == "STOP" and speed is not None and speed > 0.5:
        recs.append("проверить согласованность логики расчёта скорости и статуса привода")
    if not recs:
        recs.append("продолжить эксплуатацию в штатном режиме и наблюдать параметры в динамике")

    for idx, rec in enumerate(recs, start=1):
        lines.append(f"{idx}. {rec.capitalize()}.")

    return "\n".join(lines)

def simulate_route(df, start_lat=59.9343, start_lon=30.3351):
    lat = start_lat
    lon = start_lon
    coords = []

    prev_ts = None

    for _, row in df.iterrows():
        speed = row.get("speed_kmh_filt")
        ts = row.get("ts")

        try:
            speed = float(speed) if speed is not None and not pd.isna(speed) else 0.0
        except Exception:
            speed = 0.0

        if prev_ts is None or pd.isna(ts) or pd.isna(prev_ts):
            dt = 1.0
        else:
            dt = max(0.5, min((ts - prev_ts).total_seconds(), 5.0))

        prev_ts = ts

        # км/ч -> очень маленький шаг по координатам
        distance_factor = speed * dt / 1110000.0

        lat += distance_factor
        lon += distance_factor * 0.7

        coords.append([lat, lon])

    return pd.DataFrame(coords, columns=["lat", "lon"])
    return route

def make_route_map(route, speed_series=None):
    import plotly.graph_objects as go

    if route is None or route.empty:
        fig = go.Figure()
        fig.update_layout(
            template="plotly_dark",
            height=520,
            margin=dict(l=0, r=0, t=0, b=0),
        )
        return fig

    lat_center = float(route["lat"].mean())
    lon_center = float(route["lon"].mean())

    # Сохраняем пользовательский масштаб/позицию между автообновлениями
    if "route_map_zoom" not in st.session_state:
        st.session_state["route_map_zoom"] = 13
    if "route_map_center" not in st.session_state:
        st.session_state["route_map_center"] = {"lat": lat_center, "lon": lon_center}

    fig = go.Figure()

    # Тонкая линия маршрута
    fig.add_trace(
        go.Scattermapbox(
            lat=route["lat"],
            lon=route["lon"],
            mode="lines",
            line=dict(width=4, color="#4FC3F7"),
            name="Маршрут",
            hoverinfo="skip",
        )
    )

    # Текущая позиция
    fig.add_trace(
        go.Scattermapbox(
            lat=[route["lat"].iloc[-1]],
            lon=[route["lon"].iloc[-1]],
            mode="markers",
            marker=dict(size=14, color="#FF5252"),
            name="Текущая позиция",
            text=["Самокат"],
        )
    )

    fig.update_layout(
        mapbox=dict(
            style="carto-darkmatter",
            center=st.session_state["route_map_center"],
            zoom=st.session_state["route_map_zoom"],
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        height=520,
        template="plotly_dark",
        showlegend=False,
        uirevision="route-map-stable",
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

components.html(
    """
    <script>
    const doc = window.parent.document;

    function saveScroll() {
        sessionStorage.setItem("m365_scroll_y", String(window.parent.scrollY || doc.documentElement.scrollTop || 0));
    }

    window.parent.addEventListener("scroll", saveScroll);

    const savedY = sessionStorage.getItem("m365_scroll_y");
    if (savedY !== null) {
        setTimeout(() => {
            window.parent.scrollTo(0, parseInt(savedY, 10));
        }, 50);
    }
    </script>
    """,
    height=0,
)

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
if mode.startswith("Live") and st.session_state.get("active_tab", 0) == 0:
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
status, color, issues = scooter_health(last)


def fmt_num(v, d=2):
    import math
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    return f"{float(v):.{d}f}"

if "active_tab" not in st.session_state:
    st.session_state.active_tab = 0

tab_names = ["Панель приборов", "Состояние самоката", "Маршрут и пробег"]

selected_tab = st.radio(
    "",
    tab_names,
    index=st.session_state.active_tab,
    horizontal=True
)

st.session_state.active_tab = tab_names.index(selected_tab)


# ================================
# ВКЛАДКА 1 — ПАНЕЛЬ ПРИБОРОВ
# ================================
if st.session_state.active_tab == 0:

    st.markdown("## Панель приборов")

    g1, g2, g3, g4, g5 = st.columns(5)

    g1.plotly_chart(
        make_gauge(
            last.get("speed_kmh_filt"),
            "Speed",
            0,
            50,
            "km/h",
            thresholds=[
                {'range': [0, 20], 'color': "lightgreen"},
                {'range': [20, 35], 'color': "yellow"},
                {'range': [35, 50], 'color': "red"},
            ],
        ),
        use_container_width=True,
    )

    soc_percent = (last.get("soc") or 0) * 100

    g2.plotly_chart(
        make_gauge(
            soc_percent,
            "SOC",
            0,
            100,
            "%",
            thresholds=[
                {'range': [0, 20], 'color': "red"},
                {'range': [20, 60], 'color': "yellow"},
                {'range': [60, 100], 'color': "lightgreen"},
            ],
        ),
        use_container_width=True,
    )

    g3.plotly_chart(
        make_gauge(
            last.get("u_batt_v"),
            "U batt",
            11,
            13,
            "V",
            thresholds=[
                {'range': [11, 11.5], 'color': "red"},
                {'range': [11.5, 12.2], 'color': "yellow"},
                {'range': [12.2, 13], 'color': "lightgreen"},
            ],
        ),
        use_container_width=True,
    )

    g4.plotly_chart(
        make_gauge(
            last.get("t_batt_c"),
            "T batt",
            0,
            80,
            "°C",
            thresholds=[
                {'range': [0, 50], 'color': "lightgreen"},
                {'range': [50, 65], 'color': "yellow"},
                {'range': [65, 80], 'color': "red"},
            ],
        ),
        use_container_width=True,
    )

    g5.plotly_chart(
        make_gauge(
            last.get("i_batt_a"),
            "I batt",
            -5,
            20,
            "A",
            thresholds=[
                {'range': [-5, 0], 'color': "lightblue"},
                {'range': [0, 10], 'color': "lightgreen"},
                {'range': [10, 15], 'color': "yellow"},
                {'range': [15, 20], 'color': "red"},
            ],
        ),
        use_container_width=True,
    )

    # -------- ESP / MQTT поля --------
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

    # -------- графики --------

    c1, c2 = st.columns(2)

    c1.plotly_chart(
        make_line(df, "u_batt_v", "Battery Voltage", "V"),
        use_container_width=True,
    )

    c2.plotly_chart(
        make_line(df, "i_batt_a", "Battery Current", "A"),
        use_container_width=True,
    )

    c3, c4 = st.columns(2)

    c3.plotly_chart(
        make_line(df, "speed_kmh_filt", "Speed", "km/h"),
        use_container_width=True,
    )

    c4.plotly_chart(
        make_line(df, "t_batt_c", "Battery Temp", "°C"),
        use_container_width=True,
    )

    st.plotly_chart(
        make_line(df, "t_ctrl_c", "Controller Temp", "°C"),
        use_container_width=True,
    )

    st.plotly_chart(
        make_line(df, "soc", "SOC (0..1)"),
        use_container_width=True,
    )

    # -------- последние сообщения --------
    st.markdown("### Последние сообщения")
    st.dataframe(df.tail(50), use_container_width=True)



# ================================
# ВКЛАДКА 2 — СОСТОЯНИЕ САМОКАТА
# ================================
if st.session_state.active_tab == 1:
    st.header("Состояние электросамоката")

    status, color, issues = scooter_health(last)

    scooter_img = colorize_scooter("scooter.jpg", color)

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        st.image(scooter_img, width=500)

    if color == "green":
        st.success("Состояние: ОТЛИЧНО")

    elif color == "yellow":
        st.warning("Состояние: ХОРОШО")

    else:
        st.error("Состояние: ПЛОХО")

    st.subheader("Автоматический отчет")

    report = build_detailed_report(df, last)
    st.text(report)

if issues:
        st.markdown("### Выявленные проблемы")

        for issue in issues:
            st.write("•", issue)

if st.session_state.active_tab == 2:
    st.header("Маршрут и запас хода")

    range_km = last.get("range_km")

    if range_km is None:
        st.info("Недостаточно данных для расчёта пробега")
    else:
        st.subheader("Оставшийся пробег")

        col1, col2 = st.columns([1, 2])

        with col1:
            st.metric("Estimated Range", f"{range_km:.1f} km")

        with col2:
            max_range = 30
            progress = min(range_km / max_range, 1.0)
            st.progress(progress)

        components.html(
            render_motion_bar(last.get("speed_kmh_filt"), max_speed=speed_max_kmh),
            height=130,
            scrolling=False,
        )

    st.subheader("Карта поездки")

    route = simulate_route(df)

    if not route.empty:
        fig_map = make_route_map(route)
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("Недостаточно данных для построения маршрута")