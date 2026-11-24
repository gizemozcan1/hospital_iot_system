from datetime import datetime, timezone
import json
import os
import random
import time

import paho.mqtt.client as mqtt

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "mosquitto")
MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "1883"))
DEVICE_ID = os.getenv("DEVICE_ID", "node_01")
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", "3.0"))
TOPIC = f"hospital/device/{DEVICE_ID}/telemetry"


def connect_with_retry() -> mqtt.Client:
    while True:
        try:
            client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, keepalive=60)
            client.loop_start()
            print(f"[SIM] Connected to MQTT broker at {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}", flush=True)
            return client
        except Exception as exc:
            print(f"[SIM] Waiting for MQTT broker ({exc})...", flush=True)
            time.sleep(2.0)


def main() -> None:
    client = connect_with_retry()
    battery = 92.0

    while True:
        payload = {
            "device_id": DEVICE_ID,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature": round(random.uniform(36.2, 37.2), 1),
            "battery_percentage": round(battery, 1),
            "status": "OK",
        }
        client.publish(TOPIC, json.dumps(payload), qos=1)
        print(f"[SIM] Published to {TOPIC}: {payload}", flush=True)

        battery = max(10.0, battery - 0.2)
        time.sleep(PUBLISH_INTERVAL)


if __name__ == "__main__":
    main()