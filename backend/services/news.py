import os
import time
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

# -------------------------------
# CONFIG
# -------------------------------
NEWSAPI_ENDPOINT = "https://newsapi.org/v2/everything"

TICKER_QUERY_MAP = {
    "RELIANCE.BSE": "Reliance Industries",
    "INFY.BSE": "Infosys",
    "HDFCBANK.BSE": "HDFC Bank",
    "TCS.BSE": "Tata Consultancy Services",
}

# If ticker suffix varies (e.g., .BSE vs .NS), fall back to base symbol mapping.
BASE_TICKER_QUERY_MAP = {
    "RELIANCE": "Reliance Industries",
    "INFY": "Infosys",
    "HDFCBANK": "HDFC Bank",
    "TCS": "Tata Consultancy Services",
    "ICICIBANK": "ICICI Bank",
}

DEFAULT_PAGE_SIZE = 5


# -------------------------------
# INTERNAL HELPERS
# -------------------------------
def _fetch_newsapi(
    query: str,
    from_date: datetime,
    to_date: datetime,
    page_size: int,
    api_key: str
) -> List[Dict[str, Any]]:
    params = {
        "q": query,
        "from": from_date.strftime("%Y-%m-%d"),
        "to": to_date.strftime("%Y-%m-%d"),
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": page_size,
        "apiKey": api_key,
    }

    try:
        resp = requests.get(NEWSAPI_ENDPOINT, params=params, timeout=10)
        if resp.status_code != 200:
            return []

        articles = resp.json().get("articles", [])
        return [
            {
                "title": a.get("title"),
                "published_at": a.get("publishedAt"),
                "url": a.get("url"),
                "source": a.get("source", {}).get("name"),
            }
            for a in articles
            if a.get("title")
        ]
    except Exception:
        return []


def _deduplicate(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    unique = []
    for a in articles:
        key = (a.get("title") or "").lower().strip()
        if key and key not in seen:
            seen.add(key)
            unique.append(a)
    return unique


def _base_symbol(ticker: str) -> str:
    # Handle formats like RELIANCE.BSE / RELIANCE.NS / ^NSEI
    if not ticker:
        return ""
    if ticker.startswith("^"):
        return ticker
    return ticker.split(".", 1)[0].upper().strip()


# -------------------------------
# PUBLIC API (USED BY KAFKA)
# -------------------------------
def fetch_news_for_tickers(
    tickers: List[str],
    lookback_days: int = 3,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> Dict[str, List[Dict[str, Any]]]:

    api_key = os.getenv("NEWSAPI_KEY")
    if not api_key:
        print("[WARN] NEWSAPI_KEY missing → skipping news fetch")
        return {t: [] for t in tickers}

    to_date = datetime.utcnow()
    from_date = to_date - timedelta(days=lookback_days)

    results: Dict[str, List[Dict[str, Any]]] = {}

    for ticker in tickers:
        base = _base_symbol(ticker)
        query = (
            TICKER_QUERY_MAP.get(ticker)
            or BASE_TICKER_QUERY_MAP.get(base)
            or base
            or ticker
        )

        articles = _fetch_newsapi(
            query=query,
            from_date=from_date,
            to_date=to_date,
            page_size=page_size,
            api_key=api_key,
        )

        articles = _deduplicate(articles)
        articles.sort(key=lambda x: x.get("published_at", ""), reverse=True)

        results[ticker] = articles[:page_size]

        # VERY IMPORTANT → avoid rate limit
        time.sleep(0.7)

    return results
