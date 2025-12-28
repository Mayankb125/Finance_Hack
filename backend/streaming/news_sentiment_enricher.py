import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer

# Fix import path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.news import fetch_news_for_tickers
from services.sentiment import analyze_texts

load_dotenv()


class NewsSentimentEnricher:
    def __init__(self):
        self.input_topic = "market.stocks"
        self.output_topic = "market.enriched"

        kafka_conf = {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.getenv("KAFKA_API_KEY"),
            "sasl.password": os.getenv("KAFKA_API_SECRET"),
        }

        self.consumer = Consumer({
            **kafka_conf,
            "group.id": "news-sentiment-enricher-group",
            "auto.offset.reset": "latest",
        })

        self.producer = Producer(kafka_conf)
        self.consumer.subscribe([self.input_topic])

        print("============================================================")
        print("🧠 Kafka News + Sentiment Enricher Started")
        print(f"📥 Input topic : {self.input_topic}")
        print(f"📤 Output topic: {self.output_topic}")
        print(f"🕒 Start time (UTC): {datetime.utcnow().isoformat()}")
        print("============================================================")

    def delivery_report(self, err, msg):
        if err:
            print(f"[KAFKA ERROR] {err}")
        else:
            print(f"[ENRICHED → KAFKA] {msg.key().decode()} sent")

    def enrich_event(self, event: dict) -> dict:
        symbol = event.get("symbol")
        enriched = dict(event)

        try:
            news = fetch_news_for_tickers([symbol], lookback_days=3).get(symbol, [])
            headlines = [n["title"] for n in news if n.get("title")]

            sentiments = analyze_texts(headlines)
            avg_score = (
                sum(s["score"] for s in sentiments) / len(sentiments)
                if sentiments else 0.0
            )

            enriched["news"] = news[:5]
            enriched["sentiment"] = {
                "avg_score": round(avg_score, 4),
                "headline_count": len(headlines),
                "labels": [s["label"] for s in sentiments],
            }

        except Exception as e:
            enriched["sentiment"] = {
                "avg_score": 0.0,
                "headline_count": 0,
                "labels": [],
                "error": str(e),
            }

        enriched["enriched_at"] = datetime.utcnow().isoformat()
        return enriched

    def run(self):
        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    print(f"[KAFKA ERROR] {msg.error()}")
                    continue

                try:
                    event = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError:
                    print("[WARN] Invalid JSON event")
                    continue

                enriched_event = self.enrich_event(event)

                self.producer.produce(
                    topic=self.output_topic,
                    key=enriched_event["symbol"],
                    value=json.dumps(enriched_event),
                    on_delivery=self.delivery_report,
                )
                self.producer.flush()

        except KeyboardInterrupt:
            print("\n🛑 Enricher stopped by user")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    enricher = NewsSentimentEnricher()
    enricher.run()
