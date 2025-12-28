from __future__ import annotations

from typing import List, Dict, Any
import threading

# ------------------------------
# Global objects (thread-safe)
# ------------------------------
_model_lock = threading.Lock()
_pipeline = None

_cache_lock = threading.Lock()
_results_cache: Dict[str, Dict[str, Any]] = {}
_RESULTS_CACHE_MAX = 1000

# Limit batch size to avoid blocking Kafka consumer
MAX_BATCH_SIZE = 8


# ------------------------------
# Load FinBERT lazily
# ------------------------------
def _load_pipeline():
    global _pipeline
    if _pipeline is not None:
        return _pipeline

    with _model_lock:
        if _pipeline is None:
            from transformers import (
                AutoTokenizer,
                AutoModelForSequenceClassification,
                TextClassificationPipeline,
            )

            model_name = "ProsusAI/finbert"
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            model = AutoModelForSequenceClassification.from_pretrained(model_name)

            _pipeline = TextClassificationPipeline(
                model=model,
                tokenizer=tokenizer,
                return_all_scores=True,
                device=-1  # CPU-safe (important for hackathon infra)
            )

    return _pipeline


# ------------------------------
# Convert label → numeric score
# ------------------------------
def score_to_numeric(label: str, confidence: float) -> float:
    """
    POSITIVE → +confidence
    NEGATIVE → -confidence
    NEUTRAL  → 0
    """
    label = label.upper()
    if label == "POSITIVE":
        return confidence
    if label == "NEGATIVE":
        return -confidence
    return 0.0


# ------------------------------
# Main sentiment function
# ------------------------------
def analyze_texts(texts: List[str]) -> List[Dict[str, Any]]:
    """
    Analyze a list of texts using FinBERT.

    Returns:
    [
        {
            "label": "POSITIVE|NEGATIVE|NEUTRAL",
            "confidence": float,
            "score": float  # in [-1, +1]
        }
    ]
    """
    if not texts:
        return []

    pipe = _load_pipeline()
    results: List[Dict[str, Any] | None] = []

    to_score: List[str] = []
    to_score_idx: List[int] = []

    # ------------------------------
    # Cache lookup
    # ------------------------------
    with _cache_lock:
        for i, text in enumerate(texts):
            key = text.strip()[:200]
            if key in _results_cache:
                results.append(_results_cache[key])
            else:
                results.append(None)
                to_score.append(text)
                to_score_idx.append(i)

    # ------------------------------
    # Run FinBERT in safe batches
    # ------------------------------
    if to_score:
        scored_outputs = []

        for i in range(0, len(to_score), MAX_BATCH_SIZE):
            batch = to_score[i : i + MAX_BATCH_SIZE]
            scored_outputs.extend(pipe(batch, truncation=True))

        for idx, scores in enumerate(scored_outputs):
            best = max(scores, key=lambda s: s["score"]) if scores else {
                "label": "NEUTRAL",
                "score": 0.0,
            }

            out = {
                "label": best["label"],
                "confidence": float(best["score"]),
                "score": float(score_to_numeric(best["label"], float(best["score"]))),
            }

            result_index = to_score_idx[idx]
            results[result_index] = out

            key = to_score[idx].strip()[:200]
            with _cache_lock:
                if len(_results_cache) >= _RESULTS_CACHE_MAX:
                    _results_cache.pop(next(iter(_results_cache)))
                _results_cache[key] = out

    return results
