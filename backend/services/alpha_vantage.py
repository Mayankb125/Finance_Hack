import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class AlphaVantageService:
    BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self):
        self.api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        if not self.api_key:
            raise RuntimeError("ALPHA_VANTAGE_API_KEY not found")

        # keep SMALL to avoid rate limit
        self.stocks = [
            "RELIANCE.BSE",
            "TCS.BSE",
            "INFY.BSE",
            "HDFCBANK.BSE",
            "ICICIBANK.BSE"
        ]

        print("[OK] AlphaVantageService initialized")

    def fetch_stock(self, symbol):
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key
        }

        r = requests.get(self.BASE_URL, params=params, timeout=10)
        data = r.json().get("Global Quote", {})

        if not data:
            return None

        return {
            "symbol": symbol,
            "price": float(data["05. price"]),
            "change": float(data["09. change"]),
            "changePercent": data["10. change percent"],
            "timestamp": datetime.utcnow().isoformat()
        }

    def fetch_all_stocks(self):
        results = []
        for s in self.stocks:
            try:
                stock = self.fetch_stock(s)
                if stock:
                    print(f"[OK] {s} → {stock['price']}")
                    results.append(stock)
            except Exception as e:
                print(f"[ERROR] {s}: {e}")
        return results
