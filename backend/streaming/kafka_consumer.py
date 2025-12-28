import os
import sys
import json
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer

# Fix import path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()


class MarketKafkaConsumer:
    def __init__(self):
        self.topic = "market.stocks"

        self.conf = {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.getenv("KAFKA_API_KEY"),
            "sasl.password": os.getenv("KAFKA_API_SECRET"),
            "group.id": "market-consumer-group-1",
            "auto.offset.reset": "earliest",
        }

        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([self.topic])

        print("============================================================")
        print("📥 Kafka Consumer Started")
        print(f"🔗 Topic: {self.topic}")
        print(f"🕒 Start time (UTC): {datetime.utcnow().isoformat()}")
        print("============================================================")

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
                    data = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError:
                    print(f"[WARN] Invalid JSON message: {msg.value().decode('utf-8')[:100]}")
                    continue

                print("\n📩 New Market Event Received")
                print("----------------------------------")
                print(f"Symbol     : {data.get('symbol', 'N/A')}")
                print(f"Price      : {data.get('price', 'N/A')}")
                print(f"Change     : {data.get('change', 'N/A')}")
                print(f"Change %   : {data.get('changePercent', 'N/A')}")
                print(f"Timestamp  : {data.get('timestamp', 'N/A')}")
                print("----------------------------------")

        except KeyboardInterrupt:
            print("\n🛑 Consumer stopped by user")

        finally:
            self.consumer.close()


if __name__ == "__main__":
    consumer = MarketKafkaConsumer()
    consumer.run()
