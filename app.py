#!/usr/bin/env python3
"""
app.py - FastAPI Kafka Dashboard (map + table)

- Consumes Kafka topic 'iot_video' (hardcoded default)
- Extracts Vehicle_Movement events
- Serves a WebSocket at /ws that streams latest updates
- Serves a dashboard page at /

Run:
  pip install fastapi uvicorn[standard] aiokafka python-multipart
  python app.py

Env overrides (optional):
  KAFKA_BOOTSTRAP=localhost:9092
  KAFKA_TOPIC=iot_video
  APP_HOST=0.0.0.0
  APP_PORT=8000
"""

import os
import json
import ast
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
from aiokafka import AIOKafkaConsumer

# ---------- CONFIG (hardcoded defaults with env overrides) ----------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot_video")
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = int(os.getenv("APP_PORT", "5001"))

# ---------- FastAPI app ----------
app = FastAPI(title="Vehicle Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files (index.html + assets)
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ---------- In-memory state & broadcaster ----------
latest_per_device: Dict[str, Dict[str, Any]] = {}
clients: List[WebSocket] = []
clients_lock = asyncio.Lock()

async def broadcast(event: Dict[str, Any]):
    """Send JSON event to all connected websockets."""
    dead = []
    async with clients_lock:
        for ws in clients:
            try:
                await ws.send_text(json.dumps(event))
            except Exception:
                dead.append(ws)
        for ws in dead:
            try:
                await ws.close()
            except Exception:
                pass
            clients.remove(ws)


# ---------- Parsing helpers ----------
def parse_record(raw: str) -> Optional[Dict[str, Any]]:
    """Parse JSON first, then Python literal (single quotes)."""
    raw = raw.strip()
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        pass
    try:
        obj = ast.literal_eval(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return None

def to_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def extract_vehicle_movement(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Pull fields if this is a Vehicle_Movement event."""
    if msg.get("event") != "Vehicle_Movement":
        return None

    device_id = str(msg.get("device_id", "unknown"))
    ts = msg.get("timestamp")
    try:
        ts_iso = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone().isoformat()
    except Exception:
        ts_iso = str(ts)

    data = msg.get("data") or {}
    lat = to_float(data.get("latitude"))
    lon = to_float(data.get("longitude"))
    speed = to_float(data.get("speed"))
    movement = data.get("movement")  # acceleration/deceleration/constant
    state = data.get("state")

    if lat is None or lon is None:
        # If coordinates are not numeric, drop it
        return None

    return {
        "device_id": device_id,
        "timestamp": ts_iso,
        "speed": speed,              # assume km/h
        "latitude": lat,
        "longitude": lon,
        "movement": movement,
        "state": state,
        # You can add more fields here if needed
    }


# ---------- Kafka consumer background task ----------
consumer_task: Optional[asyncio.Task] = None
consumer: Optional[AIOKafkaConsumer] = None

async def kafka_loop():
    global consumer
    while True:
        try:
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                enable_auto_commit=True,
                auto_offset_reset="latest",
                value_deserializer=lambda m: m.decode("utf-8", errors="ignore"),
            )
            await consumer.start()
            # Poll loop
            async for msg in consumer:
                parsed = parse_record(msg.value)
                if not parsed:
                    continue
                extracted = extract_vehicle_movement(parsed)
                if not extracted:
                    continue
                dev = extracted["device_id"]
                latest_per_device[dev] = extracted
                await broadcast({"type": "update", "data": extracted})
        except Exception as e:
            # Backoff and retry on errors
            print(f"[Kafka] error: {e}. Retrying in 3s...")
            await asyncio.sleep(3)
        finally:
            if consumer:
                try:
                    await consumer.stop()
                except Exception:
                    pass

@app.on_event("startup")
async def on_startup():
    global consumer_task
    consumer_task = asyncio.create_task(kafka_loop())

@app.on_event("shutdown")
async def on_shutdown():
    if consumer_task:
        consumer_task.cancel()
        with contextlib.suppress(Exception):
            await consumer_task


# ---------- HTTP & WebSocket endpoints ----------
@app.get("/", response_class=HTMLResponse)
async def index():
    # Serve static/index.html
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    # Fallback small page if file missing
    return HTMLResponse("<h1>Dashboard not found</h1><p>Put index.html in /static</p>")

@app.get("/api/state")
async def api_state():
    """Return latest per device snapshot."""
    return JSONResponse({"devices": list(latest_per_device.values())})

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    async with clients_lock:
        clients.append(websocket)
    # On connect, send the current snapshot
    await websocket.send_text(json.dumps({"type": "snapshot", "data": list(latest_per_device.values())}))
    try:
        while True:
            # Keep the connection alive; we don't expect messages from client
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with clients_lock:
            if websocket in clients:
                clients.remove(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host=APP_HOST, port=APP_PORT, reload=False)
