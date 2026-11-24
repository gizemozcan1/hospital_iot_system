from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import os
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import paho.mqtt.client as mqtt

from database import SessionLocal, TelemetryLog, init_db

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "mosquitto")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
TELEMETRY_TOPIC = "hospital/device/+/telemetry"

latest_telemetry: Optional[dict] = None
mqtt_client: Optional[mqtt.Client] = None


def parse_timestamp(ts_value: Optional[str]) -> datetime:
    if not ts_value:
        return datetime.now(timezone.utc)
    try:
        normalized = ts_value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return datetime.now(timezone.utc)


def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None) -> None:
    if reason_code == 0 or getattr(reason_code, "value", 1) == 0:
        client.subscribe(TELEMETRY_TOPIC, qos=1)
        print(f"[MQTT] Connected to {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT} and subscribed to {TELEMETRY_TOPIC}", flush=True)
    else:
        print(f"[MQTT] Connection failed with code: {reason_code}", flush=True)


def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
    global latest_telemetry
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        device_id = payload.get("device_id", "node_01")
        measured_at = parse_timestamp(payload.get("timestamp"))
        temperature = float(payload["temperature"])
        battery_percentage = float(payload["battery_percentage"])
        status = str(payload.get("status", "OK"))
        last_seen_dt = datetime.now(timezone.utc)

        with SessionLocal() as db:
            log_entry = TelemetryLog(
                device_id=device_id,
                timestamp=measured_at,
                temperature=temperature,
                battery_percentage=battery_percentage,
                status=status,
            )
            db.add(log_entry)
            db.commit()
            db.refresh(log_entry)

        latest_telemetry = {
            "id": log_entry.id,
            "device_id": device_id,
            "temperature": temperature,
            "battery_percentage": battery_percentage,
            "status": status,
            "measured_at": measured_at.isoformat(),
            "last_seen": last_seen_dt.isoformat(),
        }
        print(
            f"[DB] Saved telemetry id={log_entry.id} device={device_id} "
            f"temp={temperature} bat={battery_percentage}",
            flush=True,
        )
    except Exception as exc:
        print(f"[MQTT/DB ERROR] Failed to process message on {msg.topic}: {exc}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global mqtt_client
    init_db()

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
    client.loop_start()
    mqtt_client = client

    try:
        yield
    finally:
        if mqtt_client is not None:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()


app = FastAPI(title="Hospital IoT Walking Skeleton", lifespan=lifespan)


@app.get("/api/latest")
def get_latest():
    if latest_telemetry is not None:
        return latest_telemetry

    with SessionLocal() as db:
        row = db.query(TelemetryLog).order_by(TelemetryLog.id.desc()).first()
        if not row:
            return JSONResponse(status_code=200, content={"status": "No data received yet"})
        return {
            "id": row.id,
            "device_id": row.device_id,
            "temperature": row.temperature,
            "battery_percentage": row.battery_percentage,
            "status": row.status,
            "measured_at": row.timestamp.isoformat(),
            "last_seen": row.timestamp.isoformat(),
        }


@app.get("/", response_class=HTMLResponse)
def plain_monitor_page():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <title>Hospital IoT - Walking Skeleton</title>
    </head>
    <body>
      <h1>Hospital IoT Device Monitor (Walking Skeleton)</h1>
      <div id="content">Loading latest telemetry...</div>

      <script>
        async function refreshData() {
          try {
            const res = await fetch("/api/latest");
            const data = await res.json();
            const el = document.getElementById("content");
            if (data.status === "No data received yet") {
              el.innerHTML = "<p>No data received yet.</p>";
              return;
            }
            el.innerHTML = `
              <p><strong>Device ID:</strong> ${data.device_id}</p>
              <p><strong>Latest Temperature:</strong> ${data.temperature} &deg;C</p>
              <p><strong>Latest Battery:</strong> ${data.battery_percentage}%</p>
              <p><strong>Last Seen:</strong> ${data.last_seen}</p>
            `;
          } catch (err) {
            document.getElementById("content").innerHTML = "<p>Error loading data.</p>";
          }
        }
        refreshData();
        setInterval(refreshData, 2000);
      </script>
    </body>
    </html>
    """