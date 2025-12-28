import os
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

FMP_API_KEY = os.getenv("FMP_API_KEY")
BASE_URL = "https://financialmodelingprep.com/api/v3"

if not FMP_API_KEY:
    raise RuntimeError("FMP_API_KEY not found in .env")

class MarketDataService:
    """
    Market data service using Financial Modeling Prep (FMP)
    Safe for scheduled fetching (15 min intervals)
    """

    def __init__(self):
        # ===============================
        # NIFTY 50 STOCK SYMBOLS
        # ===============================
        self.stocks = [
            "RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ICICIBANK.NS","ITC.NS","LT.NS",
            "SBIN.NS","BHARTIARTL.NS","AXISBANK.NS","KOTAKBANK.NS","HCLTECH.NS","WIPRO.NS",
            "ASIANPAINT.NS","HINDUNILVR.NS","MARUTI.NS","SUNPHARMA.NS","ULTRACEMCO.NS",
            "BAJAJFINSV.NS","BAJFINANCE.NS","ADANIENT.NS","ADANIPORTS.NS","TITAN.NS",
            "NESTLEIND.NS","TATASTEEL.NS","JSWSTEEL.NS","POWERGRID.NS","NTPC.NS",
            "COALINDIA.NS","M&M.NS","TATAMOTORS.NS","EICHERMOT.NS","HEROMOTOCO.NS",
            "CIPLA.NS","DRREDDY.NS","BRITANNIA.NS","DIVISLAB.NS","HDFCLIFE.NS",
            "SBILIFE.NS","GRASIM.NS","SHREECEM.NS","BPCL.NS","HINDALCO.NS","ONGC.NS",
            "APOLLOHOSP.NS","BAJAJ-AUTO.NS","TATACONSUM.NS","TECHM.NS",
            "INDUSINDBK.NS","UPL.NS"
        ]

        # Indices
        self.indices = ["^NSEI", "^BSESN"]

        print("[OK] MarketDataService (FMP) initialized")
        print(f"  - Stocks: {len(self.stocks)}")
        print("  - Indices: NIFTY 50, SENSEX")

    # --------------------------------------------------
    # INTERNAL: BATCH QUOTE FETCH
    # --------------------------------------------------
    def _fetch_quotes(self, symbols):
        symbol_str = ",".join(symbols)
        url = f"{BASE_URL}/quote/{symbol_str}"
        params = {"apikey": FMP_API_KEY}

        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[ERROR] FMP fetch failed: {e}")
            return []

    # --------------------------------------------------
    # STOCK DATA
    # --------------------------------------------------
    def get_all_stocks(self):
        print("📈 Fetching stock prices from FMP...")
        raw_data = self._fetch_quotes(self.stocks)

        stocks = []
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for item in raw_data:
            try:
                stocks.append({
                    "symbol": item.get("symbol"),
                    "name": item.get("name"),
                    "price": round(item.get("price", 0), 2),
                    "previousClose": round(item.get("previousClose", 0), 2),
                    "change": round(item.get("change", 0), 2),
                    "changePercent": round(item.get("changesPercentage", 0), 2),
                    "dayHigh": round(item.get("dayHigh", 0), 2),
                    "dayLow": round(item.get("dayLow", 0), 2),
                    "open": round(item.get("open", 0), 2),
                    "volume": item.get("volume", 0),
                    "marketCap": item.get("marketCap", 0),
                    "lastUpdate": now,
                    "source": "FMP"
                })
            except Exception:
                continue

        print(f"[OK] Fetched {len(stocks)} stocks")
        return stocks

    # --------------------------------------------------
    # INDEX DATA
    # --------------------------------------------------
    def get_all_indices(self):
        print("📊 Fetching index data from FMP...")
        raw_data = self._fetch_quotes(self.indices)

        indices = {}
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for item in raw_data:
            symbol = item.get("symbol")
            name = "NIFTY 50" if symbol == "^NSEI" else "SENSEX"

            indices[name.lower().replace(" ", "")] = {
                "symbol": symbol,
                "name": name,
                "price": round(item.get("price", 0), 2),
                "previousClose": round(item.get("previousClose", 0), 2),
                "change": round(item.get("change", 0), 2),
                "changePercent": round(item.get("changesPercentage", 0), 2),
                "dayHigh": round(item.get("dayHigh", 0), 2),
                "dayLow": round(item.get("dayLow", 0), 2),
                "open": round(item.get("open", 0), 2),
                "lastUpdate": now,
                "source": "FMP"
            }

        print("[OK] Index data fetched")
        return indices
