import time
import json
import os
import sys
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
from confluent_kafka import Producer

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.alpha_vantage import AlphaVantageService

load_dotenv()


class MarketKafkaProducer:
    def __init__(self):
        self.topic = "market.stocks"

        self.conf = {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.getenv("KAFKA_API_KEY"),
            "sasl.password": os.getenv("KAFKA_API_SECRET"),
        }

        self.producer = Producer(self.conf)
        self.market_service = AlphaVantageService()

        print("============================================================")
        print("🚀 Market Streaming Backend Started")
        print("🔗 Mode: Alpha Vantage → Kafka")
        print("⏱ Fetch interval: 15 minutes (continuous mode)")
        print(f"🕒 Start time (UTC): {datetime.utcnow().isoformat()}")
        print("📊 Tracking stocks: " + str(len(self.market_service.stocks)))
        print("============================================================")

    def delivery_report(self, err, msg):
        if err:
            print(f"[KAFKA ERROR] {err}")
        else:
            print(f"[KAFKA OK] {msg.key().decode()} → sent")

    def stream_once(self):
        print("\n------------------------------------------------------------")
        print(f"🔄 Streaming cycle @ {datetime.utcnow().isoformat()}")
        print("📈 Fetching stock prices...")

        stocks = self.market_service.fetch_all_stocks()

        for stock in stocks:
            payload = json.dumps(stock)

            self.producer.produce(
                topic=self.topic,
                key=stock["symbol"],
                value=payload,
                on_delivery=self.delivery_report
            )

        self.producer.flush()
        print(f"✅ Streamed {len(stocks)} stocks to Kafka")

    def run_forever(self, interval_minutes=15):
        cycle_count = 0
        while True:
            cycle_count += 1
            print(f"\n{'='*60}")
            print(f"🔄 CYCLE #{cycle_count} - Auto-updating all stocks...")
            print(f"{'='*60}\n")
            
            self.stream_once()
            
            next_update = datetime.utcnow() + timedelta(minutes=interval_minutes)
            print(f"\n✅ Cycle #{cycle_count} completed successfully")
            print(f"⏳ Next auto-update in {interval_minutes} minutes")
            print(f"🕒 Next update time: {next_update.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            print(f"{'='*60}\n")
            
            time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka market price producer (Alpha Vantage → market.stocks)")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single fetch+produce cycle and exit (demo/testing)",
    )
    parser.add_argument(
        "--interval-minutes",
        type=float,
        default=float(os.getenv("PRODUCER_INTERVAL_MINUTES", "15")),
        help="Interval in minutes for continuous mode (default: 15 or PRODUCER_INTERVAL_MINUTES)",
    )
    args = parser.parse_args()

    producer = MarketKafkaProducer()

    if args.once:
        producer.stream_once()
    else:
        producer.run_forever(interval_minutes=args.interval_minutes)
