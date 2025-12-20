import express from 'express';
import cors from 'cors';
import mqtt from 'mqtt';
import { InfluxDB, Point } from '@influxdata/influxdb-client';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
const port = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(express.json());

// InfluxDB клиент
const influx = new InfluxDB({
  url: process.env.INFLUX_URL,
  token: process.env.INFLUX_TOKEN,
});
const writeApi = influx.getWriteApi(process.env.INFLUX_ORG, process.env.INFLUX_BUCKET);
const queryApi = influx.getQueryApi(process.env.INFLUX_ORG);

// MQTT клиент
const mqttClient = mqtt.connect(process.env.MQTT_URL);

mqttClient.on('connect', () => {
  console.log(`[MQTT] Connected to ${process.env.MQTT_URL}`);
  mqttClient.subscribe(process.env.MQTT_TOPIC, () => {
    console.log(`[MQTT] Subscribed to topic: ${process.env.MQTT_TOPIC}`);
  });
});

mqttClient.on('message', (topic, message) => {
  try {
    const data = JSON.parse(message.toString());
    console.log('[MQTT] Message:', data);

    const point = new Point('scooter')
      .tag('device_id', data.device_id)
      .floatField('u_batt_v', data.u_batt_v)
      .floatField('i_batt_a', data.i_batt_a)
      .floatField('t_batt_c', data.t_batt_c)
      .floatField('t_ctrl_c', data.t_ctrl_c)
      .floatField('speed_kmh', data.speed_kmh)
      .floatField('ax_ms2', data.ax_ms2)
      .floatField('ay_ms2', data.ay_ms2)
      .floatField('az_ms2', data.az_ms2);

    writeApi.writePoint(point);
  } catch (err) {
    console.error('[MQTT ERROR]', err.message);
  }
});

// REST API endpoint
app.get('/api/data', async (req, res) => {
  const fluxQuery = `
    from(bucket:"${process.env.INFLUX_BUCKET}")
      |> range(start: -5m)
      |> filter(fn: (r) => r._measurement == "scooter")
      |> last()
  `;

  try {
    const rows = [];
    await queryApi.collectRows(fluxQuery, {
      next: (row) => rows.push(row),
      complete: () => res.json(rows),
      error: (error) => {
        console.error('[INFLUX ERROR]', error.message);
        res.status(500).json({ error: error.message });
      },
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Запуск сервера
app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
