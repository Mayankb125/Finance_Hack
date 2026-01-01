import os
import sys
import json
from datetime import datetime

from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer

# ------------------------------------------------------------------
# Environment & imports
# ------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

load_dotenv()

# ------------------------------------------------------------------
# Vertex AI (Gemini) – SAFE OPTIONAL IMPORT
# ------------------------------------------------------------------
try:
    from services.vertex_ai_explainer import get_gemini_explanation
    VERTEX_AVAILABLE = True
except Exception:
    VERTEX_AVAILABLE = False

    def get_gemini_explanation(*args, **kwargs):
        return "AI explanation unavailable"

# ------------------------------------------------------------------
# Market Signal Engine
# ------------------------------------------------------------------
class MarketSignalEngine:
    def __init__(self):
        self.input_topic = "market.enriched"
        self.output_topic = "market.signals"

        kafka_conf = {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.getenv("KAFKA_API_KEY"),
            "sasl.password": os.getenv("KAFKA_API_SECRET"),
        }

        self.consumer = Consumer({
            **kafka_conf,
            "group.id": "signal-engine-group",
            "auto.offset.reset": "earliest",
        })

        self.producer = Producer(kafka_conf)
        self.consumer.subscribe([self.input_topic])

        print("============================================================")
        print("🧠 Market Signal Engine Started")
        print(f"📥 Input topic : {self.input_topic}")
        print(f"📤 Output topic: {self.output_topic}")
        print(f"🕒 Start time (UTC): {datetime.utcnow().isoformat()}")
        print(f"🤖 Vertex AI     : {'ENABLED' if VERTEX_AVAILABLE else 'DISABLED'}")
        print("============================================================")

    # ------------------------------------------------------------------
    # Core signal logic
    # ------------------------------------------------------------------
    def generate_signal(self, data: dict) -> dict:
        symbol = data.get("symbol")
        price = data.get("price", 0.0)
        price_change = data.get("change", 0.0)

        sentiment_obj = data.get("sentiment") or {}

        # 🔐 Robust sentiment extraction (new + legacy)
        sentiment = (
            sentiment_obj.get("avg_score")
            if isinstance(sentiment_obj, dict)
            else None
        )

        if sentiment is None:
            sentiment = data.get("sentimentScore")

        if sentiment is None and isinstance(sentiment_obj, dict):
            sentiment = sentiment_obj.get("score")

        try:
            sentiment = float(sentiment)
        except Exception:
            sentiment = 0.0

        # --------------------------------------------------------------
        # Decision logic (Hackathon friendly)
        # --------------------------------------------------------------
        if price_change > 0 and sentiment > 0.2:
            signal = "BUY"
            confidence = min(1.0, abs(sentiment) + 0.3)
            reason = "Price rising with positive news sentiment"

        elif price_change < 0 and sentiment < -0.2:
            signal = "SELL"
            confidence = min(1.0, abs(sentiment) + 0.3)
            reason = "Price falling with negative news sentiment"

        else:
            signal = "NEUTRAL"
            confidence = 0.4
            reason = "Mixed price movement and sentiment"

        # --------------------------------------------------------------
        # Vertex AI explanation (SAFE, non-blocking)
        # --------------------------------------------------------------
        ai_explanation = "AI explanation unavailable"
        if VERTEX_AVAILABLE:
            try:
                # Extract news from enriched data
                news = data.get("news", [])
                ai_explanation = get_gemini_explanation(
                    symbol=symbol,
                    price=price,
                    sentiment=sentiment,
                    news=news,
                    price_change=price_change,
                    signal=signal
                )
                if not ai_explanation or ai_explanation.startswith("[Vertex AI"):
                    print(f"[WARN] Vertex AI returned: {ai_explanation}")
            except Exception as e:
                print(f"[WARN] Vertex AI explanation failed: {type(e).__name__}: {e}")
                import traceback
                traceback.print_exc()
                ai_explanation = f"AI explanation unavailable: {str(e)[:100]}"
        else:
            print("[INFO] Vertex AI not available (module not imported)")

        return {
            "symbol": symbol,
            "price": price,
            "change": price_change,
            "sentimentScore": round(sentiment, 4),
            "signal": signal,
            "confidence": round(confidence, 2),
            "reason": reason,
            "ai_explanation": ai_explanation,
            "timestamp": datetime.utcnow().isoformat()
        }

    # ------------------------------------------------------------------
    # Kafka callbacks
    # ------------------------------------------------------------------
    def delivery_report(self, err, msg):
        if err:
            print(f"[KAFKA ERROR] {err}")
        else:
            print(f"[SIGNAL SENT] {msg.key().decode()} → {self.output_topic}")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    def run(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    print(f"[KAFKA ERROR] {msg.error()}")
                    continue

                data = json.loads(msg.value().decode("utf-8"))
                signal_payload = self.generate_signal(data)

                self.producer.produce(
                    topic=self.output_topic,
                    key=signal_payload["symbol"],
                    value=json.dumps(signal_payload),
                    on_delivery=self.delivery_report
                )

                self.producer.flush()

                print("\n🧠 New Signal Generated")
                print("----------------------------------")
                print(f"Symbol    : {signal_payload['symbol']}")
                print(f"Signal    : {signal_payload['signal']}")
                print(f"Confidence: {signal_payload['confidence']}")
                print(f"Reason    : {signal_payload['reason']}")
                if VERTEX_AVAILABLE:
                    print(f"AI        : {signal_payload['ai_explanation']}")
                print("----------------------------------")

        except KeyboardInterrupt:
            print("\n🛑 Signal Engine stopped")

        finally:
            self.consumer.close()


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    engine = MarketSignalEngine()
    engine.run()
