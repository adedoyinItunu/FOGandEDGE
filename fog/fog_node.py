"""
fog_node.py — Fog Layer Processing Node
========================================
Subscribes to all sensor MQTT topics, performs local aggregation,
anomaly detection, and dispatches batched payloads to AWS SQS.

Responsibilities:
  1. Subscribe to sensors/# wildcard topic via MQTT
  2. Maintain a rolling window (deque) per sensor type
  3. Every DISPATCH_INTERVAL seconds: compute statistics, flag anomalies
  4. Build enriched batch payload and send to SQS

Environment variables:
    MQTT_HOST               MQTT broker hostname (default: localhost)
    MQTT_PORT               MQTT port (default: 1883)
    USE_TLS                 Use AWS IoT Core TLS (default: false)
    CA_CERT_PATH            Root CA cert path
    CERT_PATH               Device cert path
    KEY_PATH                Private key path
    SQS_QUEUE_URL           AWS SQS queue URL (required)
    AWS_REGION              AWS region (default: eu-west-1)
    FOG_NODE_ID             Unique ID for this fog node (default: fog_node_01)
    DISPATCH_INTERVAL       Seconds between SQS dispatches (default: 30)
    WINDOW_SIZE             Rolling window size per sensor (default: 20)
    ANOMALY_SIGMA           Sigma threshold for anomaly detection (default: 2.0)
"""
import os
import json
import time
import logging
import threading
import statistics
from collections import deque
from datetime import datetime, timezone

import boto3
import paho.mqtt.client as mqtt
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("FogNode")

# ─── Configuration ────────────────────────────────────────────────────────────
MQTT_HOST        = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT        = int(os.getenv("MQTT_PORT", "1883"))
USE_TLS          = os.getenv("USE_TLS", "false").lower() == "true"
CA_CERT_PATH     = os.getenv("CA_CERT_PATH")
CERT_PATH        = os.getenv("CERT_PATH")
KEY_PATH         = os.getenv("KEY_PATH")
SQS_QUEUE_URL    = os.getenv("SQS_QUEUE_URL", "")
AWS_REGION       = os.getenv("AWS_REGION", "eu-west-1")
FOG_NODE_ID      = os.getenv("FOG_NODE_ID", "fog_node_01")
DISPATCH_INTERVAL = float(os.getenv("DISPATCH_INTERVAL", "30"))
WINDOW_SIZE      = int(os.getenv("WINDOW_SIZE", "20"))
ANOMALY_SIGMA    = float(os.getenv("ANOMALY_SIGMA", "2.0"))

# ─── State ────────────────────────────────────────────────────────────────────
# Rolling window per sensor_id: deque of (timestamp, value) tuples
sensor_windows: dict[str, deque] = {}
sensor_metadata: dict[str, dict] = {}   # sensor_id -> {sensor_type, unit}
windows_lock = threading.Lock()

# SQS client (lazy init to support unit testing without AWS creds)
_sqs_client = None


def get_sqs_client():
    global _sqs_client
    if _sqs_client is None:
        _sqs_client = boto3.client("sqs", region_name=AWS_REGION)
    return _sqs_client


# ─── MQTT Callbacks ───────────────────────────────────────────────────────────

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
        client.subscribe("sensors/#", qos=1)
        logger.info("Subscribed to sensors/#")
    else:
        logger.error(f"MQTT connection failed (rc={rc})")


def on_message(client, userdata, msg):
    """Handle incoming sensor message: parse and append to rolling window."""
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        sensor_id = payload["sensor_id"]
        sensor_type = payload["sensor_type"]
        value = float(payload["value"])
        unit = payload["unit"]
        timestamp = payload["timestamp"]

        with windows_lock:
            if sensor_id not in sensor_windows:
                sensor_windows[sensor_id] = deque(maxlen=WINDOW_SIZE)
                sensor_metadata[sensor_id] = {"sensor_type": sensor_type, "unit": unit}
            sensor_windows[sensor_id].append((timestamp, value))

        logger.debug(f"Received {sensor_type}/{sensor_id}: {value} {unit}")

    except (KeyError, ValueError, json.JSONDecodeError) as e:
        logger.warning(f"Malformed message on {msg.topic}: {e}")


def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(f"Unexpected MQTT disconnect (rc={rc}). Will auto-reconnect.")


# ─── Fog Processing Logic ─────────────────────────────────────────────────────

def compute_window_stats(readings: list[float]) -> dict:
    """
    Compute descriptive statistics for a window of readings.
    Returns mean, std_dev, min, max, and count.
    """
    if not readings:
        return {"mean": None, "std_dev": None, "min": None, "max": None, "count": 0}

    mean = statistics.mean(readings)
    std_dev = statistics.stdev(readings) if len(readings) > 1 else 0.0
    return {
        "mean": round(mean, 3),
        "std_dev": round(std_dev, 3),
        "min": round(min(readings), 3),
        "max": round(max(readings), 3),
        "count": len(readings),
    }


def detect_anomalies(readings: list[float], timestamps: list[str],
                     mean: float, std_dev: float, sigma_threshold: float) -> list[dict]:
    """
    Flag readings that deviate more than sigma_threshold standard deviations
    from the window mean. Returns list of anomaly records.
    """
    if std_dev == 0 or mean is None:
        return []

    anomalies = []
    for ts, val in zip(timestamps, readings):
        z_score = abs(val - mean) / std_dev
        if z_score > sigma_threshold:
            anomalies.append({
                "timestamp": ts,
                "value": round(val, 3),
                "z_score": round(z_score, 3),
                "direction": "high" if val > mean else "low"
            })
    return anomalies


def process_windows() -> list[dict]:
    """
    Snapshot all sensor windows, compute statistics and anomalies.
    Returns a list of processed sensor summaries.
    """
    processed = []
    with windows_lock:
        snapshot = {sid: (list(w), dict(sensor_metadata[sid]))
                    for sid, w in sensor_windows.items() if len(w) > 0}

    for sensor_id, (window_data, meta) in snapshot.items():
        timestamps = [entry[0] for entry in window_data]
        values     = [entry[1] for entry in window_data]

        stats = compute_window_stats(values)
        anomalies = []
        if stats["mean"] is not None and stats["std_dev"] is not None:
            anomalies = detect_anomalies(
                values, timestamps,
                stats["mean"], stats["std_dev"],
                ANOMALY_SIGMA
            )

        processed.append({
            "sensor_id": sensor_id,
            "sensor_type": meta["sensor_type"],
            "unit": meta["unit"],
            "statistics": stats,
            "anomalies": anomalies,
            "anomaly_count": len(anomalies),
            "latest_value": values[-1] if values else None,
            "latest_timestamp": timestamps[-1] if timestamps else None,
        })

    return processed


def build_batch_payload(processed_sensors: list[dict]) -> dict:
    """Assemble the final batch payload to be sent to SQS."""
    return {
        "fog_node_id": FOG_NODE_ID,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "dispatch_window_seconds": DISPATCH_INTERVAL,
        "window_size": WINDOW_SIZE,
        "anomaly_sigma_threshold": ANOMALY_SIGMA,
        "sensor_count": len(processed_sensors),
        "sensors": processed_sensors,
        "total_anomalies": sum(s["anomaly_count"] for s in processed_sensors),
    }


def dispatch_to_sqs(payload: dict) -> bool:
    """Send the processed batch payload to AWS SQS. Returns True on success."""
    if not SQS_QUEUE_URL:
        logger.error("SQS_QUEUE_URL is not set — cannot dispatch payload.")
        return False
    try:
        body = json.dumps(payload)
        response = get_sqs_client().send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=body,
            MessageAttributes={
                "fog_node_id": {
                    "StringValue": FOG_NODE_ID,
                    "DataType": "String"
                },
                "sensor_count": {
                    "StringValue": str(payload["sensor_count"]),
                    "DataType": "Number"
                },
                "total_anomalies": {
                    "StringValue": str(payload["total_anomalies"]),
                    "DataType": "Number"
                }
            }
        )
        logger.info(f"Dispatched to SQS (MessageId={response['MessageId']}, "
                    f"sensors={payload['sensor_count']}, "
                    f"anomalies={payload['total_anomalies']})")
        return True
    except ClientError as e:
        logger.error(f"SQS dispatch failed: {e}")
        return False


# ─── Dispatcher Thread ────────────────────────────────────────────────────────

def dispatcher_loop():
    """Background thread: process windows and dispatch every DISPATCH_INTERVAL seconds."""
    logger.info(f"Dispatcher started (interval={DISPATCH_INTERVAL}s, "
                f"window={WINDOW_SIZE}, sigma={ANOMALY_SIGMA})")
    while True:
        time.sleep(DISPATCH_INTERVAL)
        processed = process_windows()
        if not processed:
            logger.info("No sensor data in windows yet — skipping dispatch.")
            continue

        payload = build_batch_payload(processed)
        dispatch_to_sqs(payload)

        # Log summary
        for s in processed:
            logger.info(
                f"  {s['sensor_type']}/{s['sensor_id']}: "
                f"mean={s['statistics']['mean']} {s['unit']}, "
                f"anomalies={s['anomaly_count']}"
            )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    # Start dispatcher thread
    dispatch_thread = threading.Thread(target=dispatcher_loop, daemon=True, name="Dispatcher")
    dispatch_thread.start()

    # Set up MQTT client
    client = mqtt.Client(client_id=FOG_NODE_ID)
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    if USE_TLS and CA_CERT_PATH:
        client.tls_set(ca_certs=CA_CERT_PATH, certfile=CERT_PATH, keyfile=KEY_PATH)

    logger.info(f"Fog node '{FOG_NODE_ID}' starting up...")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info("Fog node stopped by user.")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
