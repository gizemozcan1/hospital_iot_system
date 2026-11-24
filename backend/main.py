import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import json
import os
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import paho.mqtt.client as mqtt

from database import Alert, Device, HeartbeatLog, SessionLocal, TelemetryLog, init_db

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "mosquitto")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))

TELEMETRY_TOPIC = "hospital/device/+/telemetry"
HEARTBEAT_TOPIC = "hospital/device/+/heartbeat"

HEARTBEAT_INTERVAL_SECONDS = 5.0
MISSED_HEARTBEATS_THRESHOLD = 2
OFFLINE_TIMEOUT_SECONDS = HEARTBEAT_INTERVAL_SECONDS * MISSED_HEARTBEATS_THRESHOLD  # 10.0s
LOW_BATTERY_THRESHOLD = 15.0
RULE_CHECK_INTERVAL_SECONDS = 2.0

latest_telemetry_by_device: dict[str, dict] = {}
last_heartbeat_at_by_device: dict[str, datetime] = {}
mqtt_client: Optional[mqtt.Client] = None
offline_monitor_task: Optional[asyncio.Task] = None


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


def ensure_device_exists(db, device_id: str) -> Device:
    device = db.query(Device).filter(Device.device_id == device_id).first()
    if not device:
        default_name = "Medical Refrigerator" if device_id == "node_02" else f"Device {device_id}"
        default_loc = "Pharmacy - Cold Room 2" if device_id == "node_02" else "General Ward"
        device = Device(
            device_id=device_id,
            device_name=default_name,
            location=default_loc,
        )
        db.add(device)
        db.flush()
    return device


def upsert_active_alert(db, device_id: str, alert_type: str, description: str, now_dt: datetime) -> None:
    active = (
        db.query(Alert)
        .filter(
            Alert.device_id == device_id,
            Alert.alert_type == alert_type,
            Alert.resolved_at.is_(None),
        )
        .order_by(Alert.alert_id.desc())
        .first()
    )
    if active is None:
        alert = Alert(
            device_id=device_id,
            alert_type=alert_type,
            triggered_at=now_dt,
            resolved_at=None,
            description=description,
        )
        db.add(alert)
        print(f"[ALERT-OPEN] device={device_id} type={alert_type} desc='{description}'", flush=True)


def resolve_active_alert(db, device_id: str, alert_type: str, now_dt: datetime) -> None:
    active_alerts = (
        db.query(Alert)
        .filter(
            Alert.device_id == device_id,
            Alert.alert_type == alert_type,
            Alert.resolved_at.is_(None),
        )
        .all()
    )
    for alert in active_alerts:
        alert.resolved_at = now_dt
        print(f"[ALERT-RESOLVED] device={device_id} type={alert_type} alert_id={alert.alert_id}", flush=True)


def evaluate_telemetry_rules(db, device_id: str, temperature: float, battery_percentage: float, now_dt: datetime) -> None:
    # REQ-05: Low battery (< 15.0%)
    if battery_percentage < LOW_BATTERY_THRESHOLD:
        upsert_active_alert(
            db=db,
            device_id=device_id,
            alert_type="LOW_BATTERY",
            description=f"Battery level at {battery_percentage:.1f}% (below {LOW_BATTERY_THRESHOLD:.1f}% threshold).",
            now_dt=now_dt,
        )
    else:
        resolve_active_alert(db=db, device_id=device_id, alert_type="LOW_BATTERY", now_dt=now_dt)

    # REQ-06: Implausible / out-of-range telemetry values
    implausible_reasons = []
    if battery_percentage < 0.0 or battery_percentage > 100.0:
        implausible_reasons.append(f"battery_percentage={battery_percentage:.1f}% outside [0.0, 100.0]")

    if device_id == "node_02":
        if temperature < 0.0 or temperature > 15.0:
            implausible_reasons.append(f"refrigerator temperature={temperature:.1f}C outside [0.0, 15.0]C")
    else:
        if temperature < 0.0 or temperature > 45.0:
            implausible_reasons.append(f"sensor temperature={temperature:.1f}C outside [0.0, 45.0]C")

    if implausible_reasons:
        upsert_active_alert(
            db=db,
            device_id=device_id,
            alert_type="IMPLAUSIBLE_VALUE",
            description="Implausible telemetry value: " + "; ".join(implausible_reasons) + ".",
            now_dt=now_dt,
        )
    else:
        resolve_active_alert(db=db, device_id=device_id, alert_type="IMPLAUSIBLE_VALUE", now_dt=now_dt)


def evaluate_offline_rules(now_dt: Optional[datetime] = None) -> None:
    check_dt = now_dt or datetime.now(timezone.utc)
    cutoff_dt = check_dt - timedelta(seconds=OFFLINE_TIMEOUT_SECONDS)

    with SessionLocal() as db:
        devices = db.query(Device).all()
        for dev in devices:
            last_hb = last_heartbeat_at_by_device.get(dev.device_id)
            if last_hb is None:
                latest_hb_row = (
                    db.query(HeartbeatLog)
                    .filter(HeartbeatLog.device_id == dev.device_id)
                    .order_by(HeartbeatLog.timestamp.desc(), HeartbeatLog.id.desc())
                    .first()
                )
                if latest_hb_row is not None:
                    hb_ts = latest_hb_row.timestamp
                    if hb_ts.tzinfo is None:
                        hb_ts = hb_ts.replace(tzinfo=timezone.utc)
                    last_hb = hb_ts
                    last_heartbeat_at_by_device[dev.device_id] = hb_ts

            if last_hb is not None and last_hb < cutoff_dt:
                elapsed = (check_dt - last_hb).total_seconds()
                upsert_active_alert(
                    db=db,
                    device_id=dev.device_id,
                    alert_type="OFFLINE",
                    description=f"No heartbeat received for {elapsed:.1f}s (exceeds {OFFLINE_TIMEOUT_SECONDS:.0f}s window).",
                    now_dt=check_dt,
                )
            elif last_hb is not None and last_hb >= cutoff_dt:
                resolve_active_alert(db=db, device_id=dev.device_id, alert_type="OFFLINE", now_dt=check_dt)

        db.commit()


async def offline_monitor_loop() -> None:
    while True:
        try:
            evaluate_offline_rules()
        except Exception as exc:
            print(f"[OFFLINE-MONITOR ERROR] {exc}", flush=True)
        await asyncio.sleep(RULE_CHECK_INTERVAL_SECONDS)


def handle_telemetry_message(payload: dict, received_at: datetime) -> None:
    device_id = str(payload.get("device_id", "node_01"))
    measured_at = parse_timestamp(payload.get("timestamp"))
    temperature = float(payload["temperature"])
    battery_percentage = float(payload["battery_percentage"])
    status = str(payload.get("status", "OK"))

    with SessionLocal() as db:
        ensure_device_exists(db, device_id)
        log_entry = TelemetryLog(
            device_id=device_id,
            timestamp=measured_at,
            temperature=temperature,
            battery_percentage=battery_percentage,
            status=status,
        )
        db.add(log_entry)
        db.flush()

        evaluate_telemetry_rules(
            db=db,
            device_id=device_id,
            temperature=temperature,
            battery_percentage=battery_percentage,
            now_dt=received_at,
        )
        db.commit()
        db.refresh(log_entry)

    latest_telemetry_by_device[device_id] = {
        "id": log_entry.id,
        "device_id": device_id,
        "temperature": temperature,
        "battery_percentage": battery_percentage,
        "status": status,
        "measured_at": measured_at.isoformat(),
        "last_seen": received_at.isoformat(),
    }
    print(
        f"[DB-TEL] Saved id={log_entry.id} device={device_id} "
        f"temp={temperature} bat={battery_percentage}",
        flush=True,
    )


def handle_heartbeat_message(payload: dict, received_at: datetime) -> None:
    device_id = str(payload.get("device_id", "node_01"))
    hb_ts = parse_timestamp(payload.get("timestamp"))
    uptime_seconds = int(payload.get("uptime_seconds", 0))

    with SessionLocal() as db:
        ensure_device_exists(db, device_id)
        hb_entry = HeartbeatLog(
            device_id=device_id,
            timestamp=hb_ts,
            uptime_seconds=uptime_seconds,
        )
        db.add(hb_entry)
        resolve_active_alert(db=db, device_id=device_id, alert_type="OFFLINE", now_dt=received_at)
        db.commit()
        db.refresh(hb_entry)

    last_heartbeat_at_by_device[device_id] = received_at
    print(
        f"[DB-HB] Saved id={hb_entry.id} device={device_id} uptime={uptime_seconds}s",
        flush=True,
    )


def on_connect(client: mqtt.Client, userdata, flags, reason_code, properties=None) -> None:
    if reason_code == 0 or getattr(reason_code, "value", 1) == 0:
        client.subscribe([(TELEMETRY_TOPIC, 1), (HEARTBEAT_TOPIC, 1)])
        print(
            f"[MQTT] Connected to {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT} and subscribed to "
            f"{TELEMETRY_TOPIC} & {HEARTBEAT_TOPIC}",
            flush=True,
        )
    else:
        print(f"[MQTT] Connection failed with code: {reason_code}", flush=True)


def on_message(client: mqtt.Client, userdata, msg: mqtt.MQTTMessage) -> None:
    received_at = datetime.now(timezone.utc)
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        if msg.topic.endswith("/heartbeat"):
            handle_heartbeat_message(payload, received_at)
        elif msg.topic.endswith("/telemetry"):
            handle_telemetry_message(payload, received_at)
        else:
            print(f"[MQTT] Ignored message on unhandled topic: {msg.topic}", flush=True)
    except Exception as exc:
        print(f"[MQTT/DB ERROR] Failed to process message on {msg.topic}: {exc}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global mqtt_client, offline_monitor_task
    init_db()

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
    client.loop_start()
    mqtt_client = client

    offline_monitor_task = asyncio.create_task(offline_monitor_loop())

    try:
        yield
    finally:
        if offline_monitor_task is not None:
            offline_monitor_task.cancel()
        if mqtt_client is not None:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()


app = FastAPI(title="Hospital IoT Device Monitor", lifespan=lifespan)


def compute_connection_status(device_id: str, now_dt: datetime) -> tuple[str, Optional[str]]:
    last_hb = last_heartbeat_at_by_device.get(device_id)
    if last_hb is None:
        with SessionLocal() as db:
            row = (
                db.query(HeartbeatLog)
                .filter(HeartbeatLog.device_id == device_id)
                .order_by(HeartbeatLog.timestamp.desc(), HeartbeatLog.id.desc())
                .first()
            )
            if row is not None:
                hb_dt = row.timestamp
                if hb_dt.tzinfo is None:
                    hb_dt = hb_dt.replace(tzinfo=timezone.utc)
                last_hb = hb_dt
                last_heartbeat_at_by_device[device_id] = hb_dt

    if last_hb is None:
        return "OFFLINE", None

    elapsed = (now_dt - last_hb).total_seconds()
    conn_status = "ONLINE" if elapsed <= OFFLINE_TIMEOUT_SECONDS else "OFFLINE"
    return conn_status, last_hb.isoformat()


@app.get("/api/latest")
def get_latest():
    now_dt = datetime.now(timezone.utc)
    if latest_telemetry_by_device:
        latest_item = max(
            latest_telemetry_by_device.values(),
            key=lambda item: item.get("last_seen", ""),
        )
        conn_status, last_hb_iso = compute_connection_status(latest_item["device_id"], now_dt)
        return {
            **latest_item,
            "connection_status": conn_status,
            "last_heartbeat": last_hb_iso,
        }

    with SessionLocal() as db:
        row = db.query(TelemetryLog).order_by(TelemetryLog.id.desc()).first()
        if not row:
            return JSONResponse(status_code=200, content={"status": "No data received yet"})
        conn_status, last_hb_iso = compute_connection_status(row.device_id, now_dt)
        return {
            "id": row.id,
            "device_id": row.device_id,
            "temperature": row.temperature,
            "battery_percentage": row.battery_percentage,
            "status": row.status,
            "measured_at": row.timestamp.isoformat(),
            "last_seen": row.timestamp.isoformat(),
            "connection_status": conn_status,
            "last_heartbeat": last_hb_iso,
        }


@app.get("/api/devices")
def get_devices():
    now_dt = datetime.now(timezone.utc)
    result = []
    with SessionLocal() as db:
        devices = db.query(Device).order_by(Device.device_id.asc()).all()
        for dev in devices:
            conn_status, last_hb_iso = compute_connection_status(dev.device_id, now_dt)
            latest_tel = (
                db.query(TelemetryLog)
                .filter(TelemetryLog.device_id == dev.device_id)
                .order_by(TelemetryLog.id.desc())
                .first()
            )
            result.append(
                {
                    "device_id": dev.device_id,
                    "device_name": dev.device_name,
                    "location": dev.location,
                    "connection_status": conn_status,
                    "last_heartbeat": last_hb_iso,
                    "latest_temperature": latest_tel.temperature if latest_tel else None,
                    "latest_battery": latest_tel.battery_percentage if latest_tel else None,
                    "last_telemetry_at": latest_tel.timestamp.isoformat() if latest_tel else None,
                }
            )
    return {"devices": result}


@app.get("/api/alerts")
def get_alerts():
    with SessionLocal() as db:
        alerts = db.query(Alert).order_by(Alert.alert_id.desc()).limit(50).all()
        return {
            "alerts": [
                {
                    "alert_id": a.alert_id,
                    "device_id": a.device_id,
                    "alert_type": a.alert_type,
                    "triggered_at": a.triggered_at.isoformat(),
                    "resolved_at": a.resolved_at.isoformat() if a.resolved_at else None,
                    "is_active": a.resolved_at is None,
                    "description": a.description,
                }
                for a in alerts
            ]
        }


@app.get("/", response_class=HTMLResponse)
def plain_monitor_page():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <title>Hospital IoT Device Monitor</title>
    </head>
    <body>
      <h1>Hospital IoT Device Monitor</h1>
      <h2>Devices</h2>
      <div id="devices">Loading devices...</div>
      <h2>Recent Alerts</h2>
      <div id="alerts">Loading alerts...</div>

      <script>
        async function refreshData() {
          try {
            const [devRes, alertRes] = await Promise.all([
              fetch("/api/devices"),
              fetch("/api/alerts")
            ]);
            const devData = await devRes.json();
            const alertData = await alertRes.json();

            const devEl = document.getElementById("devices");
            if (!devData.devices || devData.devices.length === 0) {
              devEl.innerHTML = "<p>No devices registered yet.</p>";
            } else {
              devEl.innerHTML = devData.devices.map(d => `
                <div style="border:1px solid #ccc; padding:8px; margin-bottom:8px;">
                  <p><strong>Device:</strong> ${d.device_id} (${d.device_name} — ${d.location})</p>
                  <p><strong>Connection Status:</strong> ${d.connection_status}</p>
                  <p><strong>Latest Temperature:</strong> ${d.latest_temperature !== null ? d.latest_temperature + " &deg;C" : "N/A"}</p>
                  <p><strong>Latest Battery:</strong> ${d.latest_battery !== null ? d.latest_battery + "%" : "N/A"}</p>
                  <p><strong>Last Heartbeat:</strong> ${d.last_heartbeat || "Never"}</p>
                </div>
              `).join("");
            }

            const alertEl = document.getElementById("alerts");
            if (!alertData.alerts || alertData.alerts.length === 0) {
              alertEl.innerHTML = "<p>No alerts recorded.</p>";
            } else {
              alertEl.innerHTML = alertData.alerts.map(a => `
                <div style="border:1px solid #ddd; padding:6px; margin-bottom:6px;">
                  <p><strong>[${a.alert_type}]</strong> ${a.device_id} — ${a.is_active ? "ACTIVE" : "RESOLVED"}</p>
                  <p>${a.description}</p>
                  <p><small>Triggered: ${a.triggered_at}${a.resolved_at ? " | Resolved: " + a.resolved_at : ""}</small></p>
                </div>
              `).join("");
            }
          } catch (err) {
            document.getElementById("devices").innerHTML = "<p>Error loading devices.</p>";
            document.getElementById("alerts").innerHTML = "<p>Error loading alerts.</p>";
          }
        }
        refreshData();
        setInterval(refreshData, 2000);
      </script>
    </body>
    </html>
    """