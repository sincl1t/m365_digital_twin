M365 Digital Twin — Stage 2 Package
===================================

Содержимое:
- node_red_flow.json — импортируй в Node-RED (Menu → Import → из файла).
- grafana_dashboard.json — импортируй в Grafana (Dashboards → Import, укажи свой InfluxDB datasource).
- logs/raw/2025-09-26_m365_synthetic.jsonl — синтетический лог телеметрии (1 Гц) ~27 минут, 4 фазы.

MQTT:
  broker: localhost:1883 (поменяй в узле mqtt-broker-1)
  topics:
    scooter/m365-lis-01/telemetry — сообщения JSON (см. содержимое лога)
    scooter/m365-lis-01/events    — сервисные маркеры фаз

InfluxDB:
  measurement: scooter
  tags: device_id, fw_src
  fields: u_batt_v, i_batt_a, t_batt_c, t_ctrl_c, speed_kmh, ax_ms2, ay_ms2, az_ms2, lat, lon, pwr_w
  time: ts (ISO8601)

Графана:
  При импорте задай переменную datasource (DS_INFLUXDB) на свой источник InfluxDB.
  Панели: U/I, температуры, скорость/мощность; блок SOC (заглушка) — заполним на этапах 4–5.

Примечание:
  Это стартовый пакет. Для реального стрима замени источник на ESP32/BLE или INA219 и публикуй тот же JSON в MQTT.