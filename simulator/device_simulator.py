from datetime import datetime, timezone
import json
import os
import random
import threading
import time

import paho.mqtt.client as mqtt

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "mosquitto")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
TELEMETRY_INTERVAL = float(os.getenv("TELEMETRY_INTERVAL", "3.0"))
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "5.0"))

DEVICES = [
    {
        "device_id": "node_01",
        "device_name": "Infusion Pump",
        "location": "Ward A - Room 101",
        "temp_min": 36.2,
        "temp_max": 37.5,
        "battery_start": 20.0,
        "battery_step": 0.8,
        "anomaly_every": 12,
        "anomaly_temp": 48.0,
    },
    {
        "device_id": "node_02",
        "device_name": "Medical Refrigerator",
        "location": "Pharmacy - Cold Room 2",
        "temp_min": 3.0,
        "temp_max": 7.0,
        "battery_start": 88.0,
        "battery_step": 0.4,
        "anomaly_every": 15,
        "anomaly_temp": 18.5,
    },
]


def connect_with_retry() -> mqtt.Client:
    while True:
        try:
            client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
            client.loop_start()
            print(
                f"[SIM] Connected to MQTT broker at {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}",
                flush=True,
            )
            return client
        except Exception as exc:
            print(f"[SIM] Waiting for MQTT broker ({exc})...", flush=True)
            time.sleep(2.0)


def run_heartbeat_loop(client: mqtt.Client, device_cfg: dict, start_monotonic: float) -> None:
    device_id = device_cfg["device_id"]
    topic = f"hospital/device/{device_id}/heartbeat"

    while True:
        uptime_seconds = int(time.monotonic() - start_monotonic)
        payload = {
            "device_id": device_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": uptime_seconds,
        }
        client.publish(topic, json.dumps(payload), qos=1)
        print(f"[SIM-HB] Published to {topic}: {payload}", flush=True)
        time.sleep(HEARTBEAT_INTERVAL)


def run_telemetry_loop(client: mqtt.Client, device_cfg: dict) -> None:
    device_id = device_cfg["device_id"]
    topic = f"hospital/device/{device_id}/telemetry"
    battery = float(device_cfg["battery_start"])
    tick = 0

    while True:
        tick += 1
        if tick % device_cfg["anomaly_every"] == 0:
            temperature = float(device_cfg["anomaly_temp"])
        else:
            temperature = round(random.uniform(device_cfg["temp_min"], device_cfg["temp_max"]), 1)

        payload = {
            "device_id": device_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": temperature,
            "battery_percentage": round(battery, 1),
            "status": "OK",
        }
        client.publish(topic, json.dumps(payload), qos=1)
        print(f"[SIM-TEL] Published to {topic}: {payload}", flush=True)

        battery = max(5.0, round(battery - device_cfg["battery_step"], 1))
        time.sleep(TELEMETRY_INTERVAL)


def main() -> None:
    client = connect_with_retry()
    start_monotonic = time.monotonic()

    threads = []
    for dev in DEVICES:
        t_hb = threading.Thread(
            target=run_heartbeat_loop,
            args=(client, dev, start_monotonic),
            daemon=True,
        )
        t_tel = threading.Thread(
            target=run_telemetry_loop,
            args=(client, dev),
            daemon=True,
        )
        t_hb.start()
        t_tel.start()
        threads.extend([t_hb, t_tel])

    while True:
        time.sleep(1.0)


if __name__ == "__main__":
    main()