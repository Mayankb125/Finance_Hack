# 🤖 Agents Documentation (`agents.md`)

## Project: Real-Time Market Intelligence & Signal Engine

This document describes the **logical agents (microservices)** in the system, their responsibilities, Kafka topics, I/O contracts, and current status.

---

## 🧩 Agent Overview

| Agent Name | Type | Input | Output | Status |
| --- | --- | --- | --- | --- |
| Market Data Producer | Producer | Alpha Vantage API | `market.stocks` | ✅ Completed |
| News + Sentiment Enricher | Stream Processor | `market.stocks` | `market.enriched` | ✅ Completed |
| Signal Engine (+ Vertex AI) | Decision Engine | `market.enriched` | `market.signals` | ✅ Completed |
| Dashboard API Agent | WebSocket Gateway | `market.signals` | Socket.IO event: `signal` | ✅ Completed |
| Frontend Dashboard | UI Agent | Socket.IO event: `signal` | Browser UI | ✅ Completed |

---

## 🟢 Agent 1: Market Data Producer

### Description
Fetches **live stock prices** and publishes them to Kafka.

### Responsibilities
- Connect to Alpha Vantage API
- Fetch latest stock price data
- Normalize payload format
- Publish messages to Kafka topic `market.stocks`

### Input
- Alpha Vantage REST API

### Output
- Kafka topic: `market.stocks`

### Sample Payload (example)

```json
{
  "symbol": "RELIANCE.BSE",
  "price": 1559.0,
  "change": -3.5,
  "changePercent": "-0.22%",
  "timestamp": "2025-12-27T13:22:18Z"
}
```

### Status
✅ Implemented and verified

---

## 🟢 Agent 2: News + Sentiment Enricher

### Description
Enriches market data with **news + sentiment**.

### Responsibilities
- Consume stock price events from `market.stocks`
- Fetch recent company/ticker news (NewsAPI/MediaStack/Twitter if configured)
- Run FinBERT sentiment scoring on headlines
- Publish enriched events to `market.enriched`

### Input
- Kafka topic: `market.stocks`

### Output
- Kafka topic: `market.enriched`

### AI Model
- **ProsusAI/finbert** (HuggingFace Transformers)

### Enriched Payload Shape (aligned to current code)

```json
{
  "symbol": "HDFCBANK.BSE",
  "price": 992.4,
  "news": [
    {
      "source": "newsapi",
      "title": "...",
      "content": "...",
      "url": "...",
      "publishedAt": "...",
      "timestamp": "..."
    }
  ],
  "sentiment": {
    "avg_score": -0.41,
    "headline_count": 5,
    "labels": ["NEGATIVE", "NEUTRAL"]
  },
  "enriched_at": "2025-12-27T13:22:33Z"
}
```

### Status
✅ Implemented and verified

---


## 🟢 Agent 3: Signal Engine (+ Vertex AI)

### Description
Generates **actionable trading signals** by combining price movement and sentiment, and enriches each signal with an AI-generated explanation using Google Vertex AI (Gemini).

### Responsibilities
- Consume enriched events from `market.enriched`
- Apply decision logic using price change + sentiment
- Classify signal (`BUY` / `SELL` / `NEUTRAL`)
- Call Vertex AI Gemini for a natural language explanation of each signal (if configured)
- Publish final signal to `market.signals`

### Input
- Kafka topic: `market.enriched`

### Output
- Kafka topic: `market.signals`

### Signal Logic (simplified)
- Price rising + sentiment > 0.2 → `BUY`
- Price falling + sentiment < -0.2 → `SELL`
- Otherwise → `NEUTRAL`

### Sample Payload (example)

```json
{
  "symbol": "ICICIBANK.BSE",
  "price": 1350.55,
  "change": 2.1,
  "sentimentScore": 0.35,
  "signal": "BUY",
  "confidence": 0.65,
  "reason": "Price rising with positive news sentiment",
  "ai_explanation": "Based on the current price increase and positive sentiment, a BUY signal is recommended.",
  "timestamp": "2025-12-28T13:23:10Z"
}
```

### Vertex AI Integration
- Uses Google Vertex AI Gemini for explanations (requires GCP credentials and .env setup)
- Falls back to a default message if unavailable

### Status
✅ Implemented and verified (Vertex AI: code in place, requires credentials)
---

## 🟢 Agent 6: REST API Agent

### Description
Provides a REST API endpoint to fetch the latest signals for integration, monitoring, or frontend use.

### Responsibilities
- Expose `/api/signals` endpoint (Flask)
- Serve latest signals from in-memory cache

### Input
- None (HTTP GET request)

### Output
- JSON array of latest signals

### Status
✅ Implemented and verified

---

## 🟢 Agent 4: Dashboard API Agent (Flask + Socket.IO)

### Description
Bridges Kafka with the frontend using **WebSockets (Socket.IO)**.

### Responsibilities
- Consume final signals from `market.signals`
- Broadcast each signal to connected clients

### Input
- Kafka topic: `market.signals`

### Output
- Socket.IO event: `signal`

### Technologies
- Flask
- Flask-SocketIO
- Confluent Kafka Consumer

### Status
✅ Implemented and verified

---

## 🟢 Agent 5: Frontend Dashboard

### Description
Displays **live market signals** to users.

### Responsibilities
- Connect to Socket.IO backend
- Listen for `signal` events
- Render signals in a live-updating table

### Input
- Socket.IO event: `signal`

### Output
- Live browser UI

### Status
✅ Implemented and verified

---

## 🔄 Inter-Agent Communication (Kafka Topics)

```
market.stocks   → raw price events
market.enriched → price + news + sentiment
market.signals  → final trading signals
```

---


## 🚧 Planned/Utility/Test Components (Not core agents)

- Technical Indicator Agent (RSI, EMA, MACD)
- Persistence Agent (PostgreSQL / Redis)
- Alert Agent (Email / WhatsApp / Push)
- Smarter ML Signal Agent / LLM explanations
- Test/utility scripts: `test_alpha.py`, `test_kafka.py`, `kafka_test_consumer.py`, `market_data.json` (dev only)

---

## 🏁 Summary

This project follows a **true agent-based streaming architecture**:
- Each agent has **single responsibility**
- Communication is **event-driven** (Kafka)
- System is **scalable, decoupled, and fault-tolerant**
- Vertex AI (Gemini) integration for explainability (hackathon compliance)
- REST API for easy integration and monitoring
