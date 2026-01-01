import os
import time
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

        # Indian stocks (BSE) - increased list
        self.stocks = [
            "RELIANCE.BSE",
            "TCS.BSE",
            "INFY.BSE",
            "HDFCBANK.BSE",
            "ICICIBANK.BSE",
            "ITC.BSE",
            "LT.BSE",
            "SBIN.BSE",
            "BHARTIARTL.BSE",
            "AXISBANK.BSE",
            "KOTAKBANK.BSE",
            "HCLTECH.BSE",
            "WIPRO.BSE",
            "ASIANPAINT.BSE",
            "HINDUNILVR.BSE",
            "MARUTI.BSE",
            "SUNPHARMA.BSE",
            "ULTRACEMCO.BSE",
            "BAJAJFINSV.BSE",
            "BAJFINANCE.BSE"
        ]

        print("[OK] AlphaVantageService initialized")

    def fetch_stock(self, symbol):
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key
        }

        r = requests.get(self.BASE_URL, params=params, timeout=10)
        response_data = r.json()
        
        # Debug: Check for API errors
        if "Error Message" in response_data:
            print(f"[ERROR] Alpha Vantage API Error for {symbol}: {response_data['Error Message']}")
            return None
        
        if "Note" in response_data:
            print(f"[WARN] Alpha Vantage Rate Limit for {symbol}: {response_data['Note']}")
            return None
        
        data = response_data.get("Global Quote", {})

        if not data:
            return None

        # Extract all relevant fields
        price = float(data.get("05. price", 0))
        change = float(data.get("09. change", 0))
        change_percent = data.get("10. change percent", "0%")
        previous_close = float(data.get("08. previous close", 0))
        last_trading_day = data.get("07. latest trading day", "N/A")

        return {
            "symbol": symbol,
            "price": price,
            "change": change,  # This is already signed (positive = up, negative = down)
            "changePercent": change_percent,
            "previousClose": previous_close,
            "lastTradingDay": last_trading_day,
            "timestamp": datetime.utcnow().isoformat()
        }

    def fetch_all_stocks(self):
        results = []
        total = len(self.stocks)
        for idx, s in enumerate(self.stocks, 1):
            try:
                stock = self.fetch_stock(s)
                if stock:
                    print(f"[OK] [{idx}/{total}] {s} → {stock['price']}")
                    results.append(stock)
                else:
                    print(f"[WARN] [{idx}/{total}] {s} → No data returned")
            except Exception as e:
                print(f"[ERROR] [{idx}/{total}] {s}: {e}")
            
            # Rate limiting: Alpha Vantage free tier allows 5 calls/minute
            # Add delay to avoid hitting rate limits
            # Note: With 20 stocks, this will take ~4 minutes. Consider upgrading API tier for faster updates.
            if idx < total:  # Don't delay after last stock
                time.sleep(12)  # 12 seconds between calls = 5 calls per minute (free tier limit)
        
        print(f"[OK] Fetched {len(results)}/{total} stocks successfully")
        return results
