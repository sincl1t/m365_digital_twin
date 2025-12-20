// api/src/index.js
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import http from 'http';
import path from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer } from 'ws';
import mqtt from 'mqtt';
import { InfluxDB, Point } from '@influxdata/influxdb-client';

/* ---------- ENV ---------- */
const {
  PORT = '5000',

  INFLUX_URL = 'http://localhost:8087', // наружный порт у тебя 8087
  INFLUX_TOKEN = '',
  INFLUX_ORG = '',
  INFLUX_BUCKET = '',

  MQTT_URL = 'mqtt://localhost:1883',
  MQTT_TOPIC = 'scooter/+/telemetry',

  WRITE_TO_INFLUX = 'true', // можно выключить, если пишет Node-RED
} = process.env;

/* ---------- App & HTTP ---------- */
const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

/* ---------- Static (WS test page) ---------- */
const __dirname = path.dirname(fileURLToPath(import.meta.url));
app.use(express.static(path.join(__dirname, '../public')));

/* ---------- InfluxDB ---------- */
const influx = new InfluxDB({ url: INFLUX_URL, token: INFLUX_TOKEN });
const queryApi = influx.getQueryApi(INFLUX_ORG);
const writeApi = influx.getWriteApi(INFLUX_ORG, INFLUX_BUCKET, 's'); // second precision

/* ---------- WebSocket ---------- */
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (ws) => {
  safeSend(ws, { type: 'hello', ts: new Date().toISOString() });
});

/* ---------- MQTT (subscribe) ---------- */
const mqttClient = mqtt.connect(MQTT_URL);

mqttClient.on('connect', () => {
  console.log('[MQTT] connected:', MQTT_URL);
  mqttClient.subscribe(MQTT_TOPIC, (err) => {
    if (err) console.error('[MQTT] subscribe error:', err.message);
    else console.log('[MQTT] subscribed to', MQTT_TOPIC);
  });
});

mqttClient.on('message', (_topic, message) => {
  let obj;
  try {
    obj = JSON.parse(message.toString());
  } catch {
    return; // пропускаем не-JSON
  }

  // разошлём всем WS-клиентам лайв-событие
  broadcast({ type: 'telemetry', data: obj });

  // (опц.) пишем в Influx (если не пишет Node-RED)
  if (WRITE_TO_INFLUX.toLowerCase() === 'true') {
    try {
      const p = new Point('scooter')
        .tag('device_id', String(obj.device_id ?? 'unknown'))
        .floatField('u_batt_v', toNum(obj.u_batt_v))
        .floatField('i_batt_a', toNum(obj.i_batt_a))
        .floatField('t_batt_c', toNum(obj.t_batt_c))
        .floatField('t_ctrl_c', toNum(obj.t_ctrl_c))
        .floatField('speed_kmh', toNum(obj.speed_kmh))
        .floatField('ax_ms2', toNum(obj.ax_ms2))
        .floatField('ay_ms2', toNum(obj.ay_ms2))
        .floatField('az_ms2', toNum(obj.az_ms2))
        .stringField('fw_src', String(obj.fw_src ?? 'unknown'));

      if (obj.ts) p.timestamp(new Date(obj.ts)); // ISO UTC из эмулятора/Arduino
      writeApi.writePoint(p);
    } catch (e) {
      console.error('[Influx write error]', e.message);
    }
  }
});

/* ---------- REST API ---------- */

// health
app.get('/health', (_req, res) => {
  res.json({ ok: true, mqtt: mqttClient.connected, wsClients: wss.clients.size });
});

// последние значения по устройству (по всем полям)
app.get('/api/latest/:deviceId', async (req, res) => {
  const deviceId = req.params.deviceId;
  const range = req.query.range || '2h';

  const flux = `
    from(bucket: "${INFLUX_BUCKET}")
      |> range(start: -${range})
      |> filter(fn: (r) => r._measurement == "scooter" and r.device_id == "${deviceId}")
      |> last()
  `;

  try {
    const rows = await queryApi.collectRows(flux);
    const latest = { device_id: deviceId };
    for (const r of rows) {
      latest[r._field] = r._value;
      latest.ts = r._time;
    }
    res.json(latest);
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

// временные ряды по полям
app.get('/api/series/:deviceId', async (req, res) => {
  const deviceId = req.params.deviceId;
  const range = req.query.range || '60m';
  const fields = String(req.query.fields || 'u_batt_v,i_batt_a,speed_kmh')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  const fieldFilter = fields.map((f) => `r._field == "${f}"`).join(' or ');

  const flux = `
    from(bucket: "${INFLUX_BUCKET}")
      |> range(start: -${range})
      |> filter(fn: (r) => r._measurement == "scooter" and r.device_id == "${deviceId}" and (${fieldFilter}))
      |> keep(columns: ["_time","_value","_field"])
      |> sort(columns: ["_time"])
  `;

  try {
    const rows = await queryApi.collectRows(flux);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: String(err) });
  }
});

// тестовая ручка для записи точки (удобно для отладки)
app.post('/api/write', async (req, res) => {
  try {
    const d = req.body || {};
    const p = new Point('scooter')
      .tag('device_id', String(d.device_id ?? 'test'))
      .floatField('u_batt_v', toNum(d.u_batt_v ?? 40.1))
      .floatField('i_batt_a', toNum(d.i_batt_a ?? 1.1))
      .floatField('t_batt_c', toNum(d.t_batt_c ?? 25.0))
      .floatField('t_ctrl_c', toNum(d.t_ctrl_c ?? 27.0))
      .floatField('speed_kmh', toNum(d.speed_kmh ?? 18.0))
      .floatField('ax_ms2', toNum(d.ax_ms2 ?? 0.1))
      .floatField('ay_ms2', toNum(d.ay_ms2 ?? 0.0))
      .floatField('az_ms2', toNum(d.az_ms2 ?? 9.8))
      .stringField('fw_src', String(d.fw_src ?? 'api'));

    if (d.ts) p.timestamp(new Date(d.ts));
    writeApi.writePoint(p);
    await writeApi.flush();
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

/* ---------- Utils ---------- */
function toNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function safeSend(ws, obj) {
  try {
    if (ws.readyState === 1) ws.send(JSON.stringify(obj));
  } catch {}
}

function broadcast(obj) {
  const s = JSON.stringify(obj);
  for (const ws of wss.clients) {
    try {
      if (ws.readyState === 1) ws.send(s);
    } catch {}
  }
}

/* ---------- Start ---------- */
server.listen(Number(PORT), () => {
  console.log(`API: http://localhost:${PORT}`);
  console.log(`WS : ws://localhost:${PORT}/ws`);
});

/* ---------- Graceful shutdown ---------- */
const shutdown = async () => {
  console.log('\nShutting down…');
  try {
    await writeApi.close();
  } catch {}
  try {
    mqttClient.end(true);
  } catch {}
  server.close(() => process.exit(0));
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
