#!/usr/bin/env python3
"""
Live terminal dashboard for Kafka 'Vehicle_Movement' events.
Shows speed, position, and acceleration status per device.

Usage:
  pip install kafka-python rich
  python cli-dash.py
"""

import json
import ast
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from kafka import KafkaConsumer
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel


# ---------- CONFIG (hardcoded defaults) ----------
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "iot_video"
GROUP_ID = "vehicle-dashboard"
POLL_MS = 500
FROM_BEGINNING = False  # Set to True if you want to read the backlog


def parse_record(raw: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(raw)
    except Exception:
        pass
    try:
        obj = ast.literal_eval(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def extract_vehicle_movement(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if msg.get("event") != "Vehicle_Movement":
        return None

    device_id = str(msg.get("device_id", "unknown"))
    ts = msg.get("timestamp")
    try:
        ts_iso = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone().isoformat()
    except Exception:
        ts_iso = str(ts)

    data = msg.get("data") or {}
    try:
        speed = float(data.get("speed"))
    except Exception:
        speed = None

    return {
        "device_id": device_id,
        "speed": speed,
        "latitude": str(data.get("latitude")) if data.get("latitude") is not None else None,
        "longitude": str(data.get("longitude")) if data.get("longitude") is not None else None,
        "state": data.get("state"),
        "movement": data.get("movement"),
        "timestamp": ts_iso,
    }


def render_table(latest: Dict[str, Dict[str, Any]]) -> Panel:
    table = Table(title="Vehicle Movement (live)", expand=True)
    table.add_column("Device", no_wrap=True)
    table.add_column("Speed", justify="right")
    table.add_column("Latitude", justify="right")
    table.add_column("Longitude", justify="right")
    table.add_column("Movement", no_wrap=True)
    table.add_column("State", no_wrap=True)
    table.add_column("Last Seen", no_wrap=True)

    if not latest:
        table.add_row("—", "—", "—", "—", "—", "—", "waiting for messages…")
    else:
        for dev, rec in sorted(latest.items(), key=lambda x: x[0]):
            spd = f"{rec['speed']:.0f}" if isinstance(rec.get("speed"), (int, float)) else "—"
            table.add_row(
                rec.get("device_id", "—"),
                spd,
                rec.get("latitude", "—") or "—",
                rec.get("longitude", "—") or "—",
                rec.get("movement", "—") or "—",
                rec.get("state", "—") or "—",
                rec.get("timestamp", "—") or "—",
            )

    return Panel(table, title="Kafka Dashboard", border_style="bold")


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="earliest" if FROM_BEGINNING else "latest",
        value_deserializer=lambda m: m.decode("utf-8", errors="ignore"),
        consumer_timeout_ms=0,
    )

    console = Console()
    latest_per_device: Dict[str, Dict[str, Any]] = {}

    with Live(render_table(latest_per_device), console=console, refresh_per_second=8) as live:
        try:
            while True:
                batches = consumer.poll(timeout_ms=POLL_MS, max_records=500)
                updated = False
                for _tp, records in batches.items():
                    for rec in records:
                        parsed = parse_record(rec.value)
                        if not parsed:
                            continue
                        extracted = extract_vehicle_movement(parsed)
                        if not extracted:
                            continue
                        latest_per_device[extracted["device_id"]] = extracted
                        updated = True

                if updated:
                    live.update(render_table(latest_per_device))
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()


if __name__ == "__main__":
    main()
