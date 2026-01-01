import os
import json
import threading
from datetime import datetime

from flask import Flask, jsonify, request
from flask_socketio import SocketIO
from dotenv import load_dotenv
from confluent_kafka import Consumer

load_dotenv()

# ------------------ Flask App ------------------
app = Flask(__name__)

# ✅ VERY IMPORTANT: allow CORS
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
    logger=True,
    engineio_logger=False,
    allow_unsafe_werkzeug=True  # Fix for development server WebSocket issues
)

# ------------------ Kafka Config ------------------
KAFKA_TOPIC = "market.signals"

consumer_conf = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
    "group.id": "dashboard-consumer-group",
    "auto.offset.reset": "latest",
}

consumer = Consumer(consumer_conf)
consumer.subscribe([KAFKA_TOPIC])

# ------------------ In-memory Cache ------------------
# Keeps latest signal per symbol for simple HTTP verification.
_cache_lock = threading.Lock()
LATEST_SIGNALS = {}
MAX_SIGNALS = 200

print("============================================================")
print("🚀 Dashboard API + WebSocket Started")
print("📡 WebSocket: ws://localhost:5051")
print(f"📥 Flask consuming Kafka topic: {KAFKA_TOPIC}")
print("============================================================")

# ------------------ Kafka Consumer Thread ------------------
def consume_kafka():
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("[KAFKA ERROR]", msg.error())
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))

            symbol = data.get("symbol")
            if symbol:
                with _cache_lock:
                    LATEST_SIGNALS[symbol] = data
                    # Simple cap to avoid unbounded growth
                    if len(LATEST_SIGNALS) > MAX_SIGNALS:
                        # Drop an arbitrary oldest item (insertion order)
                        LATEST_SIGNALS.pop(next(iter(LATEST_SIGNALS)))

            print(f"📤 Emitting signal to frontend: {data.get('symbol')}")

            # 🔥 EMIT EVENT
            socketio.emit("signal", data)

        except Exception as e:
            print("[ERROR]", e)

# Run Kafka consumer in background
threading.Thread(target=consume_kafka, daemon=True).start()


# ------------------ Socket.IO Event Handlers ------------------
@socketio.on('connect')
def handle_connect():
    print(f"✅ Client connected: {request.sid}")
    # Send cached signals to newly connected client
    with _cache_lock:
        if LATEST_SIGNALS:
            for signal in LATEST_SIGNALS.values():
                socketio.emit("signal", signal, room=request.sid)

@socketio.on('disconnect')
def handle_disconnect():
    print(f"❌ Client disconnected: {request.sid}")


# ------------------ HTTP Endpoints (Judge-Friendly) ------------------
@app.get("/api/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "kafka_topic": KAFKA_TOPIC,
            "websocket_port": 5051,
            "cached_symbols": len(LATEST_SIGNALS),
            "server_time": datetime.utcnow().isoformat(),
        }
    )


@app.get("/api/signals")
def api_signals():
    with _cache_lock:
        values = list(LATEST_SIGNALS.values())

    # Sort by timestamp if present (newest first)
    def _ts(item):
        return item.get("timestamp") or ""

    values.sort(key=_ts, reverse=True)
    return jsonify(values)

# ------------------ Run Server ------------------
if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5051)
