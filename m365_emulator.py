import json
import time
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

# ===== НАСТРОЙКИ =====
LOG_FILE = "logs/raw/2025-09-26_m365_synthetic.jsonl"  # путь к твоему логу
BROKER = "localhost"   # IP брокера MQTT (если Node-RED на том же компе — localhost)
PORT = 1883            # стандартный порт MQTT
TOPIC = "scooter/m365-lis-01/telemetry"
DELAY = 1.0            # задержка между сообщениями в секундах (1 Гц стрим)

# ===== ПОДКЛЮЧЕНИЕ К MQTT =====
client = mqtt.Client(client_id="m365_emulator", callback_api_version=CallbackAPIVersion.VERSION2)
client.connect(BROKER, PORT, 60)
print(f"[INFO] Connected to MQTT broker at {BROKER}:{PORT}")
print(f"[INFO] Streaming to topic: {TOPIC}")

# ===== ЧТЕНИЕ И ОТПРАВКА =====
with open(LOG_FILE, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            print(f"[WARN] Некорректная строка: {line}")
            continue
        
        # Если в строке есть маркер фазы — публикуем в topic events
        if "marker" in data:
            marker_topic = "scooter/m365-lis-01/events"
            client.publish(marker_topic, json.dumps(data))
            print(f"[MARKER] {data['marker']} -> {marker_topic}")
        else:
            client.publish(TOPIC, json.dumps(data))
            print(f"[SEND] ts={data['ts']} speed={data.get('speed_kmh', '?')} km/h")

        time.sleep(DELAY)

print("[INFO] Finished streaming log.")
