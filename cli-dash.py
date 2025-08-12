#!/usr/bin/env python3
"""
Live terminal dashboard for Kafka 'Vehicle_Movement' events.
Shows speed, position, and acceleration status per device.

Usage:
  pip install kafka-python rich
  python kafka_vehicle_dashboard.py --bootstrap localhost:9092 --topic iot_video
"""

import argparse
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
        "movement": data.get("movement"),  # acceleration / deceleration / constant
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
    ap = argparse.ArgumentParser(description="Live Kafka dashboard for Vehicle_Movement events.")
    ap.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers (host:port)")
    ap.add_argument("--topic", default="iot_video", help="Kafka topic to read")
    ap.add_argument("--group-id", default="vehicle-dashboard", help="Consumer group id")
    ap.add_argument("--from-beginning", action="store_true", help="Start from earliest offsets")
    ap.add_argument("--poll-ms", type=int, default=500, help="Poll interval in milliseconds")
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        value_deserializer=lambda m: m.decode("utf-8", errors="ignore"),
        consumer_timeout_ms=0,
    )

    console = Console()
    latest_per_device: Dict[str, Dict[str, Any]] = {}

    with Live(render_table(latest_per_device), console=console, refresh_per_second=8) as live:
        try:
            while True:
                batches = consumer.poll(timeout_ms=args.poll_ms, max_records=500)
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
