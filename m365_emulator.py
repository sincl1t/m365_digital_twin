#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
M365 Digital Twin • Real-time MQTT Emulator
Автор: Григорьева Елизавета
Описание:
  - Публикует синтетическую телеметрию электросамоката в MQTT.
  - Метка времени: реальный UTC (ISO8601, сек. точность).
  - Формат payload совместим с Node-RED → InfluxDB → Streamlit/Grafana.
  - Поддерживает циклические фазы поездки (flat / incline / rough / dynamics).
"""

import json
import math
import random
import signal
import sys
import time
from argparse import ArgumentParser
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

# ---------------------------
# Вспомогательное: логгер
# ---------------------------
def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------
# Генерация синтетики
# ---------------------------
class RidePhases:
    """Простые фазы поездки с разной скоростью и «нагрузкой»."""
    def __init__(self, flat_s=60, incline_s=60, rough_s=60, dynamics_s=60):
        self.phases = [
            ("flat",     flat_s,     18.0),
            ("incline",  incline_s,  14.0),
            ("rough",    rough_s,    12.0),
            ("dynamics", dynamics_s, 22.0),
        ]
        self.total = sum(d for _, d, _ in self.phases)

    def target_speed(self, t_sec: int) -> float:
        t = t_sec % self.total
        acc = 0
        for name, dur, spd in self.phases:
            if t < acc + dur:
                return spd
            acc += dur
        return 18.0


class Emulator:
    def __init__(self, device_id: str):
        self.device_id = device_id
        # Начальные значения и «медленный дрейф»
        self.u_batt_v = 40.5
        self.i_batt_a = 0.8
        self.t_batt_c = 25.0
        self.t_ctrl_c = 27.0
        # «гравитация» по Z около 9.8, X/Y — небольшие ускорения дороги
        self.ax_ms2 = 0.05
        self.ay_ms2 = 0.03
        self.az_ms2 = 9.80
        self.seq = 0
        self.phases = RidePhases()

    def step(self, t_sec: int) -> dict:
        # Целевая скорость от фазы + немного шума
        target_speed = self.phases.target_speed(t_sec)
        speed_kmh = max(0.0, random.gauss(target_speed, 0.6))

        # Ток/напряжение: чем больше скорость — тем выше ток, падение напряжения под нагрузкой
        load = speed_kmh / 25.0  # 0..~1
        self.i_batt_a = max(0.2, 0.5 + 6.0 * load + random.gauss(0, 0.15))
        self.u_batt_v = 42.0 - 0.02 * self.seq - 0.25 * load + random.gauss(0, 0.03)
        self.u_batt_v = max(36.5, min(self.u_batt_v, 42.2))

        # Температуры: медленный рост при нагрузке
        self.t_batt_c += 0.003 * self.i_batt_a + random.gauss(0, 0.01)
        self.t_ctrl_c += 0.004 * self.i_batt_a + random.gauss(0, 0.015)

        # Ускорения: чутка шума по X/Y, Z ≈ 9.8
        self.ax_ms2 = random.gauss(0.0, 0.35)
        self.ay_ms2 = random.gauss(0.0, 0.35)
        self.az_ms2 = random.gauss(9.81, 0.12)

        # Метка времени (реальный UTC)
        ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

        self.seq += 1
        return {
            "ts": ts,
            "device_id": self.device_id,
            "u_batt_v": round(self.u_batt_v, 2),
            "i_batt_a": round(self.i_batt_a, 2),
            "t_batt_c": round(self.t_batt_c, 1),
            "t_ctrl_c": round(self.t_ctrl_c, 1),
            "speed_kmh": round(speed_kmh, 1),
            "ax_ms2": round(self.ax_ms2, 2),
            "ay_ms2": round(self.ay_ms2, 2),
            "az_ms2": round(self.az_ms2, 2),
            "fw_src": "synthetic",
            "seq": self.seq,
        }


# ---------------------------
# MQTT клиент
# ---------------------------
class MQTTStreamer:
    def __init__(self, host: str, port: int, topic: str, keepalive: int = 30):
        self.host = host
        self.port = port
        self.topic = topic
        self.keepalive = keepalive
        self.client = mqtt.Client(client_id=f"m365_emulator_{int(time.time())}")
        self.client.enable_logger()  # логи paho в stdout (приглушённые)

    def connect(self):
        log(f"MQTT: подключаюсь к {self.host}:{self.port}")
        self.client.connect(self.host, self.port, keepalive=self.keepalive)
        self.client.loop_start()

    def publish_json(self, payload: dict, qos: int = 0, retain: bool = False):
        data = json.dumps(payload, ensure_ascii=False)
        res = self.client.publish(self.topic, data, qos=qos, retain=retain)
        res.wait_for_publish()
        return data


# ---------------------------
# main
# ---------------------------
def parse_args():
    p = ArgumentParser(description="M365 Digital Twin MQTT Emulator")
    p.add_argument("--host", default="localhost", help="MQTT broker host (default: localhost)")
    p.add_argument("--port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    p.add_argument("--device", default="m365-lis-01", help="Device ID/tag (default: m365-lis-01)")
    p.add_argument("--topic-base", default="scooter", help="Base topic (default: scooter)")
    p.add_argument("--hz", type=float, default=1.0, help="Publishing frequency Hz (default: 1.0)")
    p.add_argument("--log-jsonl", default="", help="Optional: path to write JSONL stream")
    return p.parse_args()


def main():
    args = parse_args()
    topic = f"{args.topic_base}/{args.device}/telemetry"
    period = 1.0 / max(0.1, args.hz)

    emu = Emulator(device_id=args.device)
    mq = MQTTStreamer(args.host, args.port, topic)
    mq.connect()

    log(f"Публикую в MQTT: topic='{topic}', hz={args.hz}")
    log_fh = open(args.log_jsonl, "a", encoding="utf-8") if args.log_jsonl else None

    running = True

    def handle_sig(*_):
        nonlocal running
        running = False
        log("Завершаю по запросу...")

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    t0 = time.time()
    while running:
        t_sec = int(time.time() - t0)
        msg = emu.step(t_sec)
        data = mq.publish_json(msg)
        log(f"[SEND] ts={msg['ts']}  speed={msg['speed_kmh']:.1f} km/h")
        if log_fh:
            log_fh.write(data + "\n")
            log_fh.flush()
        time.sleep(period)

    if log_fh:
        log_fh.close()
    log("Эмулятор остановлен.")

if __name__ == "__main__":
    main()
