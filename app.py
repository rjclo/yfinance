from __future__ import annotations

import csv
import contextlib
import hashlib
import io
import json
import logging
import math
import os
import pickle
import re
import sqlite3
import threading
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from flask import Flask, g, jsonify, render_template, request

from fetch_market_data import (
    diagnose_source_failure,
    fetch_price_history_by_source,
    get_upstream_usage_context,
    reset_upstream_usage_context,
    set_upstream_usage_context,
)

app = Flask(__name__)


def _deserialize_cached_df(blob: bytes) -> pd.DataFrame | None:
    if not blob:
        return None
    try:
        return pd.read_parquet(io.BytesIO(blob))
    except Exception:
        pass
    try:
        obj = pickle.loads(blob)
        return obj if isinstance(obj, pd.DataFrame) else None
    except Exception:
        return None


def _serialize_cached_df(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    try:
        df.to_parquet(buf, index=True, compression="snappy")
        return buf.getvalue()
    except Exception:
        return pickle.dumps(df, protocol=pickle.HIGHEST_PROTOCOL)

# Map UI ranges to upstream fetch period/interval values.
RANGE_MAP: Dict[str, Tuple[str, str]] = {
    "1d": ("1d", "1m"),
    "1w": ("20d", "5m"),
    "1m": ("60d", "15m"),
    "3m": ("180d", "1h"),
    "1y": ("2y", "1d"),
    "max": ("max", "1d"),
}

TRADING_DAY_TARGETS: Dict[str, int | None] = {
    "1d": 1,
    "1w": 10,
    "1m": 21,
    "3m": 63,
    "1y": 252,
    "max": None,
}

BASE_DIR = Path(__file__).resolve().parent
DOTENV_PATH = BASE_DIR / ".env"
PORTFOLIO_DIR = BASE_DIR / "data" / "imported_portfolios"
PORTFOLIO_FILES_DIR = PORTFOLIO_DIR / "files"
PORTFOLIO_CONFIG_PATH = PORTFOLIO_DIR / "portfolios.json"

LOG_DIR = BASE_DIR / "data" / "logs"
SERVER_LOG_PATH = LOG_DIR / "server.log"
CLIENT_ACTION_LOG_PATH = LOG_DIR / "client_actions.jsonl"
RSI_MONITOR_LOG_PATH = LOG_DIR / "rsi_monitor.jsonl"
UPSTREAM_API_LOG_PATH = LOG_DIR / "upstream_api_calls.jsonl"

CATEGORY_DIR = BASE_DIR / "data" / "categories"
CATEGORY_CONFIG_PATH = CATEGORY_DIR / "categories.json"

AI_SETTINGS_PATH = BASE_DIR / "data" / "ai_settings.json"

CACHE_DIR = BASE_DIR / "data" / "cache"
CACHE_DB_PATH = CACHE_DIR / "market_data.db"
WATCHLIST_CACHE_DIR = CACHE_DIR / "watchlist"
CACHE_MAX_BYTES = 50 * 1024 * 1024  # 50 MB
CACHE_MAX_AGE_SEC = 7 * 24 * 3600   # 7 days
MODEL_DIR = BASE_DIR / "data" / "models"
DAILY_SCANNER_MODEL_VERSION = "lgbm_daily_v3"

RSI_MONITOR_INTERVAL_SEC = int(os.getenv("RSI_MONITOR_INTERVAL_SEC", "600"))
RSI_PERIOD = 14

_rsi_monitor_lock = threading.Lock()
_rsi_monitor_stop = threading.Event()
_rsi_monitor_thread: threading.Thread | None = None
_rsi_monitor_state: dict = {
    "running": False,
    "interval_sec": RSI_MONITOR_INTERVAL_SEC,
    "last_run_utc": None,
    "watchlist": [],
    "latest": {},
    "events": [],
    "last_error": "",
}
_history_cache_lock = threading.Lock()
_history_cache: dict[tuple[str, str, str, bool, str], dict] = {}
_news_cache_lock = threading.Lock()
_news_cache: dict[str, dict] = {}
_twelve_meta_cache_lock = threading.Lock()
_twelve_meta_cache: dict[tuple[str, str], dict] = {}
_symbol_search_cache_lock = threading.Lock()
_symbol_search_cache: dict[str, dict] = {}
_watchlist_cache_lock = threading.Lock()
_watchlist_cache: dict[str, dict] = {}
_twelve_rate_limit_lock = threading.Lock()
_twelve_rate_limit_hits: deque[float] = deque()

WATCHLIST_SCAN_DELAY_SEC = float(os.getenv("WATCHLIST_SCAN_DELAY_SEC", "1.4"))
WATCHLIST_BACKGROUND_SCAN_DELAY_SEC = float(os.getenv("WATCHLIST_BACKGROUND_SCAN_DELAY_SEC", "60"))
TWELVE_RATE_LIMIT_PER_MIN = max(1, int(os.getenv("TWELVE_RATE_LIMIT_PER_MIN", "7")))
TWELVE_RATE_LIMIT_WINDOW_SEC = 60.0

_CONFIG_ENV_KEYS = (
    "MARKET_DATA_SOURCE",
    "TWELVE_DATA_API_KEY",
    "FINNHUB_API_KEY",
    "OPENAI_API_KEY",
    "GEMINI_API_KEY",
    "GROQ_API_KEY",
    "ANTHROPIC_API_KEY",
    "CHART_AI_PROVIDER",
    "CHART_AI_MODEL",
)


def _get_market_data_source() -> str:
    source = _get_config_value("MARKET_DATA_SOURCE", "twelve_data").strip().lower()
    return source if source in {"twelve_data"} else "twelve_data"


def _parse_dotenv_text(text: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        value = value.strip()
        if len(value) >= 2 and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'")):
            value = value[1:-1]
        parsed[key] = value
    return parsed


def _read_dotenv_values() -> dict[str, str]:
    try:
        if DOTENV_PATH.exists():
            return _parse_dotenv_text(DOTENV_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _format_dotenv_value(value: str) -> str:
    text = str(value or "")
    if re.fullmatch(r"[A-Za-z0-9_./:@+-]+", text):
        return text
    return json.dumps(text, ensure_ascii=False)


def _write_dotenv_values(values: dict[str, str]) -> None:
    existing_lines: list[str] = []
    if DOTENV_PATH.exists():
        try:
            existing_lines = DOTENV_PATH.read_text(encoding="utf-8").splitlines()
        except Exception:
            existing_lines = []

    managed = {k: str(v).strip() for k, v in (values or {}).items() if k in _CONFIG_ENV_KEYS and str(v).strip()}
    seen: set[str] = set()
    output_lines: list[str] = []

    for raw_line in existing_lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in raw_line:
            output_lines.append(raw_line)
            continue
        key, _ = raw_line.split("=", 1)
        key = key.strip()
        if key not in _CONFIG_ENV_KEYS:
            output_lines.append(raw_line)
            continue
        seen.add(key)
        if key in managed:
            output_lines.append(f"{key}={_format_dotenv_value(managed[key])}")

    for key in _CONFIG_ENV_KEYS:
        if key in managed and key not in seen:
            output_lines.append(f"{key}={_format_dotenv_value(managed[key])}")

    DOTENV_PATH.write_text(("\n".join(output_lines).rstrip() + "\n") if output_lines else "", encoding="utf-8")


def _sync_dotenv_from_process_env() -> None:
    file_values = _read_dotenv_values()
    merged = dict(file_values)
    changed = False
    for key in _CONFIG_ENV_KEYS:
        env_val = os.environ.get(key)
        if env_val is None:
            continue
        env_val = env_val.strip()
        if env_val == (file_values.get(key) or "").strip():
            continue
        changed = True
        if env_val:
            merged[key] = env_val
        else:
            merged.pop(key, None)
    if changed:
        try:
            _write_dotenv_values(merged)
        except Exception:
            pass


def _get_config_value(key: str, default: str = "") -> str:
    env_val = os.environ.get(key, "").strip()
    if env_val:
        return env_val
    file_val = (_read_dotenv_values().get(key) or "").strip()
    return file_val or default


def _set_config_values(values: dict[str, str]) -> None:
    current = _read_dotenv_values()
    updated = dict(current)
    for key in _CONFIG_ENV_KEYS:
        raw = str(values.get(key, "") or "").strip()
        if raw:
            updated[key] = raw
            os.environ[key] = raw
        else:
            updated.pop(key, None)
            os.environ.pop(key, None)
    _write_dotenv_values(updated)


_sync_dotenv_from_process_env()


class _DiskCache:
    """SQLite-backed L2 cache for market data DataFrames."""

    def __init__(self, db_path: Path, max_bytes: int, max_age_sec: int):
        self._db_path = db_path
        self._max_bytes = max_bytes
        self._max_age_sec = max_age_sec
        self._lock = threading.Lock()
        self._ensure_db()

    def _ensure_db(self):
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS price_cache (
                    ticker   TEXT NOT NULL,
                    period   TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    prepost  INTEGER NOT NULL,
                    fetched_at REAL NOT NULL,
                    data     BLOB NOT NULL,
                    PRIMARY KEY (ticker, period, interval, prepost)
                )
            """)

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self._db_path), timeout=5)

    def get(self, ticker: str, period: str, interval: str, prepost: bool, max_age_sec: float) -> tuple[pd.DataFrame | None, float]:
        """Return (DataFrame, fetched_at) or (None, 0) if miss/stale."""
        with self._lock:
            try:
                with self._connect() as conn:
                    row = conn.execute(
                        "SELECT data, fetched_at FROM price_cache WHERE ticker=? AND period=? AND interval=? AND prepost=?",
                        (ticker, period, interval, int(prepost)),
                    ).fetchone()
            except Exception:
                return None, 0.0
        if row is None:
            return None, 0.0
        blob, fetched_at = row
        if (time.time() - fetched_at) > max_age_sec:
            return None, fetched_at
        df = _deserialize_cached_df(blob)
        if df is None:
            return None, 0.0
        return df, fetched_at

    def get_stale(self, ticker: str, period: str, interval: str, prepost: bool, max_stale_sec: float = 86400) -> pd.DataFrame | None:
        """Return DataFrame even if stale, up to max_stale_sec old. For fallback on API failure."""
        with self._lock:
            try:
                with self._connect() as conn:
                    row = conn.execute(
                        "SELECT data, fetched_at FROM price_cache WHERE ticker=? AND period=? AND interval=? AND prepost=?",
                        (ticker, period, interval, int(prepost)),
                    ).fetchone()
            except Exception:
                return None
        if row is None:
            return None
        blob, fetched_at = row
        if (time.time() - fetched_at) > max_stale_sec:
            return None
        return _deserialize_cached_df(blob)

    def put(self, ticker: str, period: str, interval: str, prepost: bool, df: pd.DataFrame):
        """Write DataFrame to cache. Triggers eviction if over size limit."""
        blob = _serialize_cached_df(df)
        with self._lock:
            try:
                with self._connect() as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO price_cache (ticker, period, interval, prepost, fetched_at, data) VALUES (?, ?, ?, ?, ?, ?)",
                        (ticker, period, interval, int(prepost), time.time(), blob),
                    )
            except Exception:
                return
        self._maybe_evict()

    def purge_old(self):
        """Delete rows older than max_age_sec. Called on startup."""
        cutoff = time.time() - self._max_age_sec
        with self._lock:
            try:
                with self._connect() as conn:
                    conn.execute("DELETE FROM price_cache WHERE fetched_at < ?", (cutoff,))
                    conn.execute("VACUUM")
            except Exception:
                pass

    def _maybe_evict(self):
        """If DB file exceeds max_bytes, delete oldest rows until under limit."""
        try:
            size = self._db_path.stat().st_size
        except OSError:
            return
        if size <= self._max_bytes:
            return
        with self._lock:
            try:
                with self._connect() as conn:
                    conn.execute("""
                        DELETE FROM price_cache WHERE rowid IN (
                            SELECT rowid FROM price_cache ORDER BY fetched_at ASC
                            LIMIT (SELECT MAX(1, COUNT(*) / 5) FROM price_cache)
                        )
                    """)
                    conn.execute("VACUUM")
            except Exception:
                pass


_disk_cache = _DiskCache(CACHE_DB_PATH, CACHE_MAX_BYTES, CACHE_MAX_AGE_SEC)
_disk_cache.purge_old()

_ET = ZoneInfo("America/New_York")

def _market_aware_ttl(interval: str) -> int:
    """Return TTL in seconds based on whether US market is currently open."""
    now_et = datetime.now(_ET)
    weekday = now_et.weekday()  # 0=Mon, 6=Sun
    hour_min = now_et.hour * 100 + now_et.minute

    is_market_hours = (weekday < 5) and (930 <= hour_min < 1600)

    if is_market_hours:
        return 60  # 1 minute during trading
    # Outside market hours
    if interval in ("1d", "1wk", "1mo"):
        return 12 * 3600  # 12 hours for daily+ intervals
    return 6 * 3600  # 6 hours for intraday intervals


def _extract_response_text(payload: dict) -> str:
    output = payload.get("output")
    if not isinstance(output, list):
        return ""
    chunks: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if not isinstance(block, dict):
                continue
            txt = block.get("text")
            if isinstance(txt, str) and txt.strip():
                chunks.append(txt.strip())
    return "\n\n".join(chunks).strip()


def _fetch_twelve_json_cached(endpoint: str, symbol: str, params: dict | None = None, ttl_sec: int = 3600) -> dict:
    ticker = _sanitize_ticker(symbol)
    if not ticker:
        return {}
    api_key = _get_config_value("TWELVE_DATA_API_KEY")
    if not api_key:
        return {}
    cache_key = (endpoint, ticker)
    now = time.time()
    with _twelve_meta_cache_lock:
        hit = _twelve_meta_cache.get(cache_key)
        if hit and (now - float(hit.get("at", 0.0))) < ttl_sec:
            payload = hit.get("payload")
            if isinstance(payload, dict):
                return dict(payload)
    query = {"symbol": ticker, "apikey": api_key}
    if isinstance(params, dict):
        for key, value in params.items():
            if value is None:
                continue
            query[str(key)] = str(value)
    url = f"https://api.twelvedata.com/{endpoint}?{urllib.parse.urlencode(query)}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; finlab-fundamentals/1.0)",
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            _append_upstream_api_log(
                {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "provider": "twelve_data",
                    "endpoint": endpoint,
                    "symbol": ticker,
                    "params": query,
                    "http_status": int(getattr(resp, "status", 200) or 200),
                    "usage_context": get_upstream_usage_context(),
                }
            )
            raw = resp.read().decode("utf-8")
    except Exception as exc:
        code = getattr(exc, "code", None)
        _append_upstream_api_log(
            {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "provider": "twelve_data",
                "endpoint": endpoint,
                "symbol": ticker,
                "params": query,
                "http_status": int(code) if isinstance(code, int) else None,
                "error": exc.__class__.__name__,
                "usage_context": get_upstream_usage_context(),
            }
        )
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    with _twelve_meta_cache_lock:
        _twelve_meta_cache[cache_key] = {"at": now, "payload": dict(payload)}
    return payload


def _search_twelve_symbols(query: str, ttl_sec: int = 6 * 3600, limit: int = 8) -> list[dict]:
    raw_query = str(query or "").strip()
    if len(raw_query) < 2:
        return []
    api_key = _get_config_value("TWELVE_DATA_API_KEY")
    if not api_key:
        return []
    cache_key = raw_query.lower()
    now = time.time()
    with _symbol_search_cache_lock:
        hit = _symbol_search_cache.get(cache_key)
        if hit and (now - float(hit.get("at", 0.0))) < ttl_sec:
            cached = hit.get("items")
            if isinstance(cached, list):
                return [dict(item) for item in cached[:limit] if isinstance(item, dict)]

    query_params = {"symbol": raw_query, "apikey": api_key}
    url = f"https://api.twelvedata.com/symbol_search?{urllib.parse.urlencode(query_params)}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; finlab-symbol-search/1.0)",
            "Accept": "application/json",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            _append_upstream_api_log(
                {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "provider": "twelve_data",
                    "endpoint": "symbol_search",
                    "symbol": raw_query,
                    "params": query_params,
                    "http_status": int(getattr(resp, "status", 200) or 200),
                    "usage_context": get_upstream_usage_context(),
                }
            )
            raw = resp.read().decode("utf-8")
    except Exception as exc:
        code = getattr(exc, "code", None)
        _append_upstream_api_log(
            {
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "provider": "twelve_data",
                "endpoint": "symbol_search",
                "symbol": raw_query,
                "params": query_params,
                "http_status": int(code) if isinstance(code, int) else None,
                "error": exc.__class__.__name__,
                "usage_context": get_upstream_usage_context(),
            }
        )
        return []

    try:
        payload = json.loads(raw)
    except Exception:
        return []
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []

    normalized_query = raw_query.lower()

    def _score(row: dict) -> tuple:
        symbol = _sanitize_ticker(row.get("symbol"))
        name = str(row.get("instrument_name") or row.get("name") or "").strip()
        exchange = str(row.get("exchange") or "").strip().upper()
        instrument_type = str(row.get("instrument_type") or row.get("type") or "").strip().lower()
        score = 0
        if symbol.lower() == normalized_query:
            score += 120
        elif symbol.lower().startswith(normalized_query):
            score += 90
        elif normalized_query in symbol.lower():
            score += 50
        if name.lower() == normalized_query:
            score += 110
        elif name.lower().startswith(normalized_query):
            score += 80
        elif normalized_query in name.lower():
            score += 60
        if exchange in {"NASDAQ", "NYSE", "AMEX", "NYSE ARCA"}:
            score += 15
        if instrument_type in {"common stock", "stock", "etf"}:
            score += 12
        return (-score, symbol)

    items: list[dict] = []
    seen_symbols: set[str] = set()
    for row in sorted([r for r in rows if isinstance(r, dict)], key=_score):
        symbol = _sanitize_ticker(row.get("symbol"))
        if not symbol or symbol in seen_symbols:
            continue
        seen_symbols.add(symbol)
        items.append(
            {
                "symbol": symbol,
                "name": str(row.get("instrument_name") or row.get("name") or symbol).strip() or symbol,
                "exchange": str(row.get("exchange") or "").strip(),
                "type": str(row.get("instrument_type") or row.get("type") or "").strip(),
                "country": str(row.get("country") or "").strip(),
                "currency": str(row.get("currency") or "").strip(),
            }
        )
        if len(items) >= limit:
            break

    with _symbol_search_cache_lock:
        _symbol_search_cache[cache_key] = {"at": now, "items": [dict(item) for item in items]}
    return items


def _safe_float(value) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _days_until_date(date_text: str) -> int | None:
    try:
        target = datetime.fromisoformat(str(date_text)).date()
        today = datetime.now(timezone.utc).date()
        return (target - today).days
    except Exception:
        return None


def _build_tomorrow_up_context(ticker: str) -> dict:
    symbol = _sanitize_ticker(ticker)
    if not symbol:
        return {}
    with _upstream_usage_scope("ai_explain"):
        earnings_payload = _fetch_twelve_json_cached("earnings", symbol, ttl_sec=6 * 3600)
        stats_payload = _fetch_twelve_json_cached("statistics", symbol, ttl_sec=12 * 3600)
        income_payload = _fetch_twelve_json_cached("income_statement", symbol, {"period": "Quarterly"}, ttl_sec=12 * 3600)

    earnings_rows = earnings_payload.get("earnings") if isinstance(earnings_payload.get("earnings"), list) else []
    next_earnings = None
    last_report = None
    today = datetime.now(timezone.utc).date()
    for row in earnings_rows:
        if not isinstance(row, dict):
            continue
        dt_text = str(row.get("date") or "").strip()
        try:
            dt = datetime.fromisoformat(dt_text).date()
        except Exception:
            continue
        if dt >= today and next_earnings is None:
            next_earnings = row
        if dt <= today:
            last_report = row
            break

    stats = stats_payload.get("statistics") if isinstance(stats_payload.get("statistics"), dict) else {}
    valuation = stats.get("valuations_metrics") if isinstance(stats.get("valuations_metrics"), dict) else {}
    financials = stats.get("financials") if isinstance(stats.get("financials"), dict) else {}

    income_rows = income_payload.get("income_statement") if isinstance(income_payload.get("income_statement"), list) else []
    revenue_growth_qoq = None
    revenue_growth_yoy = None
    if len(income_rows) >= 2 and isinstance(income_rows[0], dict) and isinstance(income_rows[1], dict):
        latest_sales = _safe_float(income_rows[0].get("sales"))
        prev_sales = _safe_float(income_rows[1].get("sales"))
        if latest_sales and prev_sales:
            revenue_growth_qoq = ((latest_sales / prev_sales) - 1.0) * 100.0
    if len(income_rows) >= 5 and isinstance(income_rows[0], dict) and isinstance(income_rows[4], dict):
        latest_sales = _safe_float(income_rows[0].get("sales"))
        yoy_sales = _safe_float(income_rows[4].get("sales"))
        if latest_sales and yoy_sales:
            revenue_growth_yoy = ((latest_sales / yoy_sales) - 1.0) * 100.0

    return {
        "next_earnings_date": str((next_earnings or {}).get("date") or ""),
        "days_to_next_earnings": _days_until_date((next_earnings or {}).get("date") or ""),
        "last_earnings_date": str((last_report or {}).get("date") or ""),
        "last_eps_estimate": _safe_float((last_report or {}).get("eps_estimate")),
        "last_eps_actual": _safe_float((last_report or {}).get("eps_actual")),
        "last_eps_surprise_pct": _safe_float((last_report or {}).get("surprise_prc")),
        "market_cap": _safe_float(valuation.get("market_capitalization")),
        "trailing_pe": _safe_float(valuation.get("trailing_pe")),
        "forward_pe": _safe_float(valuation.get("forward_pe")),
        "peg_ratio": _safe_float(valuation.get("peg_ratio")),
        "beta": _safe_float(financials.get("beta")),
        "profit_margin": _safe_float(financials.get("profit_margins")),
        "revenue_growth_qoq_pct": revenue_growth_qoq,
        "revenue_growth_yoy_pct": revenue_growth_yoy,
    }


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _sigmoid(value: float) -> float:
    return 1.0 / (1.0 + math.exp(-value))


def _float_or_none(value) -> float | None:
    parsed = _safe_float(value)
    return parsed if parsed is not None and math.isfinite(parsed) else None


def _add_forecast_driver(drivers: list[dict], label: str, points: float, detail: str) -> float:
    if not points:
        return 0.0
    drivers.append(
        {
            "label": label,
            "points": round(points, 3),
            "direction": "bullish" if points > 0 else "bearish",
            "detail": detail,
        }
    )
    return points


def _build_local_tomorrow_forecast(metrics: dict, tomorrow_up_context: dict) -> dict:
    summary = metrics.get("summary") if isinstance(metrics.get("summary"), dict) else {}
    indicators = metrics.get("indicators") if isinstance(metrics.get("indicators"), dict) else {}
    drivers: list[dict] = []
    risks: list[str] = []
    score = 0.0
    bullish_votes = 0
    bearish_votes = 0

    start_price = _float_or_none(summary.get("start"))
    end_price = _float_or_none(summary.get("end"))
    change_pct = _float_or_none(summary.get("change_pct"))
    rsi14 = _float_or_none(indicators.get("rsi14"))
    macd = _float_or_none(indicators.get("macd"))
    macd_signal = _float_or_none(indicators.get("macd_signal"))
    macd_hist = _float_or_none(indicators.get("macd_hist"))
    ma50 = _float_or_none(indicators.get("ma50"))
    ma200 = _float_or_none(indicators.get("ma200"))
    adx14 = _float_or_none(indicators.get("adx14"))
    atr_pct_14 = _float_or_none(indicators.get("atr_pct_14"))
    rvol20 = _float_or_none(indicators.get("rvol20"))
    regime_score = _float_or_none(indicators.get("regime_score"))
    regime_confidence = _float_or_none(indicators.get("regime_confidence"))

    if change_pct is not None:
        if change_pct >= 1.5:
            score += _add_forecast_driver(drivers, "Visible-window momentum", 0.55, f"Price gained {change_pct:.2f}% across the visible window.")
            bullish_votes += 1
        elif change_pct >= 0.4:
            score += _add_forecast_driver(drivers, "Visible-window momentum", 0.3, f"Price gained {change_pct:.2f}% across the visible window.")
            bullish_votes += 1
        elif change_pct <= -1.5:
            score += _add_forecast_driver(drivers, "Visible-window momentum", -0.55, f"Price fell {change_pct:.2f}% across the visible window.")
            bearish_votes += 1
        elif change_pct <= -0.4:
            score += _add_forecast_driver(drivers, "Visible-window momentum", -0.3, f"Price fell {change_pct:.2f}% across the visible window.")
            bearish_votes += 1

    if rsi14 is not None:
        if rsi14 < 28:
            score += _add_forecast_driver(drivers, "RSI mean-reversion", 0.45, f"RSI(14) is oversold at {rsi14:.1f}.")
            bullish_votes += 1
        elif rsi14 < 35:
            score += _add_forecast_driver(drivers, "RSI pressure", -0.2, f"RSI(14) is weak at {rsi14:.1f} but not deeply washed out.")
            bearish_votes += 1
        elif rsi14 <= 45:
            score += _add_forecast_driver(drivers, "RSI pressure", -0.1, f"RSI(14) remains below midline at {rsi14:.1f}.")
            bearish_votes += 1
        elif rsi14 < 55:
            pass
        elif rsi14 <= 68:
            score += _add_forecast_driver(drivers, "RSI support", 0.18, f"RSI(14) is constructive at {rsi14:.1f}.")
            bullish_votes += 1
        elif rsi14 > 72:
            score += _add_forecast_driver(drivers, "RSI stretch", -0.18, f"RSI(14) is extended at {rsi14:.1f}.")
            bearish_votes += 1

    if macd_hist is not None:
        score += _add_forecast_driver(
            drivers,
            "MACD histogram",
            0.35 if macd_hist > 0 else -0.35 if macd_hist < 0 else 0.0,
            f"MACD histogram is {'positive' if macd_hist > 0 else 'negative' if macd_hist < 0 else 'flat'} ({macd_hist:.4f}).",
        )
        if macd_hist > 0:
            bullish_votes += 1
        elif macd_hist < 0:
            bearish_votes += 1
    if macd is not None and macd_signal is not None:
        if macd > macd_signal:
            score += _add_forecast_driver(drivers, "MACD crossover", 0.25, f"MACD ({macd:.4f}) is above signal ({macd_signal:.4f}).")
            bullish_votes += 1
        elif macd < macd_signal:
            score += _add_forecast_driver(drivers, "MACD crossover", -0.25, f"MACD ({macd:.4f}) is below signal ({macd_signal:.4f}).")
            bearish_votes += 1

    if end_price is not None and ma50 is not None:
        score += _add_forecast_driver(
            drivers,
            "Price vs 50DMA",
            0.25 if end_price > ma50 else -0.25,
            f"Price {end_price:.2f} is {'above' if end_price > ma50 else 'below'} the 50DMA ({ma50:.2f}).",
        )
        if end_price > ma50:
            bullish_votes += 1
        else:
            bearish_votes += 1
    if end_price is not None and ma200 is not None:
        score += _add_forecast_driver(
            drivers,
            "Price vs 200DMA",
            0.35 if end_price > ma200 else -0.35,
            f"Price {end_price:.2f} is {'above' if end_price > ma200 else 'below'} the 200DMA ({ma200:.2f}).",
        )
        if end_price > ma200:
            bullish_votes += 1
        else:
            bearish_votes += 1

    if regime_score is not None:
        regime_conf_factor = _clamp((regime_confidence or 50.0) / 100.0, 0.2, 1.0)
        regime_points = _clamp(regime_score, -4.0, 4.0) / 4.0 * 0.65 * regime_conf_factor
        if abs(regime_points) >= 0.05:
            score += _add_forecast_driver(
                drivers,
                "Regime state",
                regime_points,
                f"Regime score {regime_score:.0f} with confidence {(regime_confidence or 50.0):.0f}%.",
            )
            if regime_points > 0:
                bullish_votes += 1
            elif regime_points < 0:
                bearish_votes += 1

    if adx14 is not None and adx14 >= 25 and regime_score is not None and regime_score != 0:
        trend_follow_points = 0.12 if regime_score > 0 else -0.12
        score += _add_forecast_driver(
            drivers,
            "ADX trend strength",
            trend_follow_points,
            f"ADX(14) at {adx14:.1f} says the prevailing trend has strength.",
        )
        if trend_follow_points > 0:
            bullish_votes += 1
        elif trend_follow_points < 0:
            bearish_votes += 1

    if rvol20 is not None and change_pct is not None and abs(change_pct) >= 0.4:
        if rvol20 >= 1.2:
            volume_points = 0.10 if change_pct > 0 else -0.10
            score += _add_forecast_driver(
                drivers,
                "Relative volume confirmation",
                volume_points,
                f"RVOL {rvol20:.2f} is confirming the visible move.",
            )
            if volume_points > 0:
                bullish_votes += 1
            elif volume_points < 0:
                bearish_votes += 1
        elif rvol20 < 0.8:
            risks.append(f"RVOL {rvol20:.2f} is light, so the latest move has weaker participation.")

    days_to_next_earnings = _float_or_none(tomorrow_up_context.get("days_to_next_earnings"))
    last_eps_surprise_pct = _float_or_none(tomorrow_up_context.get("last_eps_surprise_pct"))
    beta = _float_or_none(tomorrow_up_context.get("beta"))
    revenue_growth_qoq = _float_or_none(tomorrow_up_context.get("revenue_growth_qoq_pct"))
    revenue_growth_yoy = _float_or_none(tomorrow_up_context.get("revenue_growth_yoy_pct"))
    profit_margin = _float_or_none(tomorrow_up_context.get("profit_margin"))

    if days_to_next_earnings is not None and days_to_next_earnings <= 2:
        risks.append(f"Next earnings are {int(days_to_next_earnings)} day(s) away; event risk can dominate technicals.")
    if last_eps_surprise_pct is not None:
        if last_eps_surprise_pct >= 5:
            score += _add_forecast_driver(drivers, "Last EPS surprise", 0.25, f"Last EPS surprise was +{last_eps_surprise_pct:.2f}%.")
            bullish_votes += 1
        elif last_eps_surprise_pct <= -5:
            score += _add_forecast_driver(drivers, "Last EPS surprise", -0.25, f"Last EPS surprise was {last_eps_surprise_pct:.2f}%.")
            bearish_votes += 1
    if revenue_growth_qoq is not None and abs(revenue_growth_qoq) >= 3:
        score += _add_forecast_driver(
            drivers,
            "Quarterly revenue trend",
            0.15 if revenue_growth_qoq > 0 else -0.15,
            f"Quarterly revenue changed {revenue_growth_qoq:.2f}% QoQ.",
        )
        if revenue_growth_qoq > 0:
            bullish_votes += 1
        else:
            bearish_votes += 1
    if revenue_growth_yoy is not None and abs(revenue_growth_yoy) >= 5:
        score += _add_forecast_driver(
            drivers,
            "Yearly revenue trend",
            0.2 if revenue_growth_yoy > 0 else -0.2,
            f"Quarterly revenue changed {revenue_growth_yoy:.2f}% YoY.",
        )
        if revenue_growth_yoy > 0:
            bullish_votes += 1
        else:
            bearish_votes += 1
    if profit_margin is not None:
        if profit_margin >= 0.15:
            score += _add_forecast_driver(drivers, "Profit margin quality", 0.1, f"Profit margin is healthy at {profit_margin:.2f}.")
            bullish_votes += 1
        elif profit_margin < 0:
            score += _add_forecast_driver(drivers, "Profit margin quality", -0.1, f"Profit margin is negative at {profit_margin:.2f}.")
            bearish_votes += 1

    score = _clamp(score, -3.5, 3.5)
    agreement = abs(bullish_votes - bearish_votes)
    direction_votes = max(bullish_votes, bearish_votes)
    if agreement < 2:
        score *= 0.55
        risks.append("Signal alignment is mixed; multiple drivers are not agreeing cleanly.")
    elif direction_votes < 4:
        score *= 0.72
    p_up = _sigmoid(score * 0.62)
    p_down = 1.0 - p_up

    confidence_penalty = 0.0
    if atr_pct_14 is not None:
        confidence_penalty += min(12.0, max(0.0, atr_pct_14) * 2.5)
    if days_to_next_earnings is not None and days_to_next_earnings <= 2:
        confidence_penalty += 15.0
    if beta is not None and beta >= 1.5:
        confidence_penalty += 8.0
    if rvol20 is not None and rvol20 < 0.8:
        confidence_penalty += 5.0

    alignment_count = sum(1 for d in drivers if abs(float(d.get("points") or 0.0)) >= 0.15)
    confidence_pct = 42.0 + (abs(score) * 11.0) + min(14.0, agreement * 3.5) + min(10.0, direction_votes * 1.5) - confidence_penalty
    if 0.44 <= p_up <= 0.56:
        confidence_pct = min(confidence_pct, 58.0)
    confidence_pct = _clamp(confidence_pct, 20.0, 90.0)

    if p_up >= 0.58:
        bias = "bullish"
    elif p_up <= 0.42:
        bias = "bearish"
    else:
        bias = "neutral"

    top_drivers = sorted(drivers, key=lambda item: abs(float(item.get("points") or 0.0)), reverse=True)[:6]
    return {
        "horizon": "next trading day",
        "bias": bias,
        "score": round(score, 3),
        "p_up_tomorrow": round(p_up, 4),
        "p_down_tomorrow": round(p_down, 4),
        "confidence_pct": round(confidence_pct, 1),
        "top_drivers": top_drivers,
        "risks": risks[:4],
        "inputs_used": {
            "start_price": start_price,
            "end_price": end_price,
            "change_pct": change_pct,
            "rsi14": rsi14,
            "macd": macd,
            "macd_signal": macd_signal,
            "macd_hist": macd_hist,
            "ma50": ma50,
            "ma200": ma200,
            "adx14": adx14,
            "atr_pct_14": atr_pct_14,
            "rvol20": rvol20,
            "regime_score": regime_score,
            "regime_confidence": regime_confidence,
            "days_to_next_earnings": days_to_next_earnings,
            "last_eps_surprise_pct": last_eps_surprise_pct,
            "revenue_growth_qoq_pct": revenue_growth_qoq,
            "revenue_growth_yoy_pct": revenue_growth_yoy,
            "profit_margin": profit_margin,
            "beta": beta,
        },
    }


def _compute_rsi_series_pd(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0.0, pd.NA)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.astype(float)
    rsi = rsi.mask((avg_loss == 0) & (avg_gain > 0), 100.0)
    rsi = rsi.mask((avg_loss == 0) & (avg_gain == 0), 50.0)
    return rsi


def _compute_macd_pd(close: pd.Series) -> tuple[pd.Series, pd.Series, pd.Series]:
    ema12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
    ema26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False, min_periods=9).mean()
    hist = macd - signal
    return macd, signal, hist


def _compute_atr_adx_pd(df: pd.DataFrame, period: int = 14) -> tuple[pd.Series, pd.Series]:
    high = pd.to_numeric(df["High"], errors="coerce")
    low = pd.to_numeric(df["Low"], errors="coerce")
    close = pd.to_numeric(df["Close"], errors="coerce")

    tr = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0.0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0.0), 0.0)

    plus_di = 100.0 * plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0.0, pd.NA)
    minus_di = 100.0 * minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0.0, pd.NA)
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0.0, pd.NA)
    adx = dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    return atr, adx


def _compute_daily_candlestick_flags(work: pd.DataFrame) -> pd.DataFrame:
    close = pd.to_numeric(work["Close"], errors="coerce")
    open_ = pd.to_numeric(work["Open"], errors="coerce")
    high = pd.to_numeric(work["High"], errors="coerce")
    low = pd.to_numeric(work["Low"], errors="coerce")

    body = (close - open_).abs()
    candle_range = (high - low).replace(0.0, pd.NA)
    upper_wick = high - pd.concat([open_, close], axis=1).max(axis=1)
    lower_wick = pd.concat([open_, close], axis=1).min(axis=1) - low

    bullish = close > open_
    bearish = close < open_
    small_body = (body / candle_range) <= 0.30
    long_lower = lower_wick >= (body * 2.0)
    long_upper = upper_wick >= (body * 2.0)
    tiny_upper = upper_wick <= (body * 0.35)
    tiny_lower = lower_wick <= (body * 0.35)
    doji_like = (body / candle_range) <= 0.12
    near_high = (high - pd.concat([open_, close], axis=1).max(axis=1)) / candle_range <= 0.20
    near_low = (pd.concat([open_, close], axis=1).min(axis=1) - low) / candle_range <= 0.20

    ret3 = close.pct_change(3)
    prior_down = ret3.shift(1) <= -0.01
    prior_up = ret3.shift(1) >= 0.01

    prev_open = open_.shift(1)
    prev_close = close.shift(1)
    prev_body = body.shift(1)
    prev_bull = bullish.shift(1).fillna(False)
    prev_bear = bearish.shift(1).fillna(False)

    hammer = small_body & long_lower & tiny_upper & prior_down
    hanging_man = small_body & long_lower & tiny_upper & prior_up
    shooting_star = small_body & long_upper & tiny_lower & prior_up

    bullish_engulfing = (
        prev_bear
        & bullish
        & (open_ <= prev_close)
        & (close >= prev_open)
        & (body >= prev_body * 1.05)
        & prior_down
    )
    bearish_engulfing = (
        prev_bull
        & bearish
        & (open_ >= prev_close)
        & (close <= prev_open)
        & (body >= prev_body * 1.05)
        & prior_up
    )

    prev2_close = close.shift(2)
    prev2_open = open_.shift(2)
    prev2_bear = (prev2_close < prev2_open).fillna(False)
    prev2_bull = (prev2_close > prev2_open).fillna(False)
    prev1_body_small = (prev_body / candle_range.shift(1).replace(0.0, pd.NA)) <= 0.35

    morning_star = (
        prev2_bear
        & prev1_body_small.fillna(False)
        & bullish
        & prior_down
        & (close >= ((prev2_open + prev2_close) / 2.0))
    )
    evening_star = (
        prev2_bull
        & prev1_body_small.fillna(False)
        & bearish
        & prior_up
        & (close <= ((prev2_open + prev2_close) / 2.0))
    )

    dragonfly_doji = doji_like & (lower_wick / candle_range >= 0.55) & (upper_wick / candle_range <= 0.10) & prior_down
    gravestone_doji = doji_like & (upper_wick / candle_range >= 0.55) & (lower_wick / candle_range <= 0.10) & prior_up
    doji_rejection = dragonfly_doji | gravestone_doji

    out = pd.DataFrame(index=work.index)
    out["pattern_bullish_engulfing"] = bullish_engulfing.astype(int)
    out["pattern_bearish_engulfing"] = bearish_engulfing.astype(int)
    out["pattern_hammer"] = hammer.astype(int)
    out["pattern_hanging_man"] = hanging_man.astype(int)
    out["pattern_shooting_star"] = shooting_star.astype(int)
    out["pattern_morning_star"] = morning_star.astype(int)
    out["pattern_evening_star"] = evening_star.astype(int)
    out["pattern_doji_rejection"] = doji_rejection.astype(int)
    out["pattern_bullish_count"] = (
        out["pattern_bullish_engulfing"]
        + out["pattern_hammer"]
        + out["pattern_morning_star"]
        + dragonfly_doji.astype(int)
    )
    out["pattern_bearish_count"] = (
        out["pattern_bearish_engulfing"]
        + out["pattern_hanging_man"]
        + out["pattern_shooting_star"]
        + out["pattern_evening_star"]
        + gravestone_doji.astype(int)
    )
    return out


def _build_regime_score_from_row(row: pd.Series) -> tuple[float | None, float | None]:
    close = _float_or_none(row.get("Close"))
    ma50 = _float_or_none(row.get("ma50"))
    ma200 = _float_or_none(row.get("ma200"))
    macd_hist = _float_or_none(row.get("macd_hist"))
    rsi14 = _float_or_none(row.get("rsi14"))
    adx14 = _float_or_none(row.get("adx14"))
    if close is None or ma50 is None or ma200 is None:
        return None, None

    score = 0.0
    score += 2.0 if close > ma200 else -2.0
    score += 1.0 if close > ma50 else -1.0
    if macd_hist is not None:
        score += 1.0 if macd_hist > 0 else -1.0 if macd_hist < 0 else 0.0
    if rsi14 is not None:
        if rsi14 >= 58:
            score += 1.0
        elif rsi14 <= 42:
            score -= 1.0

    score = _clamp(score, -4.0, 4.0)
    trend_strength = 0.0 if adx14 is None else _clamp((adx14 - 12.0) / 18.0, 0.0, 1.0)
    confidence = _clamp(45.0 + (abs(score) * 10.0) + (trend_strength * 25.0), 20.0, 100.0)
    return round(score, 0), round(confidence, 0)


def _parse_utc_date_series(values) -> pd.Series:
    return pd.to_datetime(values, utc=True, errors="coerce")


def _build_historical_fundamental_frame(ticker: str, work: pd.DataFrame) -> pd.DataFrame:
    symbol = _sanitize_ticker(ticker)
    default = pd.DataFrame(
        index=work.index,
        data={
            "days_since_last_earnings": 365.0,
            "days_to_next_earnings": 90.0,
            "eps_surprise_pct": 0.0,
            "eps_surprise_positive": 0.0,
            "revenue_growth_qoq_pct": 0.0,
            "revenue_growth_yoy_pct": 0.0,
            "net_margin_pct": 0.0,
        },
    )
    if not symbol or work.empty:
        return default

    earnings_payload = _fetch_twelve_json_cached("earnings", symbol, ttl_sec=12 * 3600)
    income_payload = _fetch_twelve_json_cached("income_statement", symbol, {"period": "Quarterly"}, ttl_sec=12 * 3600)

    earnings_rows = earnings_payload.get("earnings") if isinstance(earnings_payload.get("earnings"), list) else []
    income_rows = income_payload.get("income_statement") if isinstance(income_payload.get("income_statement"), list) else []
    if not earnings_rows and not income_rows:
        return default

    earnings_df = pd.DataFrame([row for row in earnings_rows if isinstance(row, dict)])
    income_df = pd.DataFrame([row for row in income_rows if isinstance(row, dict)])

    events: list[dict] = []
    if not earnings_df.empty:
        earnings_df = earnings_df.copy()
        earnings_df["announce_date"] = _parse_utc_date_series(earnings_df.get("date"))
        earnings_df["eps_surprise_pct"] = pd.to_numeric(earnings_df.get("surprise_prc"), errors="coerce")
        earnings_df = earnings_df.dropna(subset=["announce_date"]).sort_values("announce_date").reset_index(drop=True)
    if not income_df.empty:
        income_df = income_df.copy()
        income_df["fiscal_date_dt"] = _parse_utc_date_series(income_df.get("fiscal_date"))
        income_df["sales"] = pd.to_numeric(income_df.get("sales"), errors="coerce")
        income_df["net_income"] = pd.to_numeric(income_df.get("net_income"), errors="coerce")
        income_df = income_df.dropna(subset=["fiscal_date_dt"]).sort_values("fiscal_date_dt", ascending=False).reset_index(drop=True)
        income_df["revenue_growth_qoq_pct"] = ((income_df["sales"] / income_df["sales"].shift(-1)) - 1.0) * 100.0
        income_df["revenue_growth_yoy_pct"] = ((income_df["sales"] / income_df["sales"].shift(-4)) - 1.0) * 100.0
        income_df["net_margin_pct"] = (income_df["net_income"] / income_df["sales"].replace(0.0, pd.NA)) * 100.0

    if not earnings_df.empty:
        past_earnings_df = earnings_df[pd.to_numeric(earnings_df.get("eps_actual"), errors="coerce").notna()].copy()
        past_earnings_df = past_earnings_df.sort_values("announce_date", ascending=False).reset_index(drop=True)
        pair_count = min(len(past_earnings_df.index), len(income_df.index))
        for idx in range(pair_count):
            erow = past_earnings_df.iloc[idx]
            irow = income_df.iloc[idx] if idx < len(income_df.index) else None
            events.append(
                {
                    "announce_date": erow["announce_date"],
                    "eps_surprise_pct": _float_or_none(erow.get("eps_surprise_pct")),
                    "eps_surprise_positive": 1.0 if (_float_or_none(erow.get("eps_surprise_pct")) or 0.0) > 0 else 0.0,
                    "revenue_growth_qoq_pct": _float_or_none(irow.get("revenue_growth_qoq_pct")) if irow is not None else None,
                    "revenue_growth_yoy_pct": _float_or_none(irow.get("revenue_growth_yoy_pct")) if irow is not None else None,
                    "net_margin_pct": _float_or_none(irow.get("net_margin_pct")) if irow is not None else None,
                }
            )

    if not events:
        return default

    event_df = pd.DataFrame(events).dropna(subset=["announce_date"]).sort_values("announce_date").reset_index(drop=True)
    if event_df.empty:
        return pd.DataFrame(index=work.index)

    dates = pd.DataFrame({"date": work["date"]}).sort_values("date").reset_index()
    merged = pd.merge_asof(
        dates,
        event_df,
        left_on="date",
        right_on="announce_date",
        direction="backward",
    )
    future = pd.merge_asof(
        dates,
        event_df[["announce_date"]].rename(columns={"announce_date": "next_announce_date"}).sort_values("next_announce_date"),
        left_on="date",
        right_on="next_announce_date",
        direction="forward",
    )
    merged["days_since_last_earnings"] = (merged["date"] - merged["announce_date"]).dt.days
    merged["days_to_next_earnings"] = (future["next_announce_date"] - merged["date"]).dt.days

    out = merged.set_index("index")[
        [
            "days_since_last_earnings",
            "days_to_next_earnings",
            "eps_surprise_pct",
            "eps_surprise_positive",
            "revenue_growth_qoq_pct",
            "revenue_growth_yoy_pct",
            "net_margin_pct",
        ]
    ].sort_index()
    out = out.reindex(work.index)
    out["days_since_last_earnings"] = pd.to_numeric(out["days_since_last_earnings"], errors="coerce").fillna(365.0).clip(lower=0.0, upper=365.0)
    out["days_to_next_earnings"] = pd.to_numeric(out["days_to_next_earnings"], errors="coerce").fillna(90.0).clip(lower=0.0, upper=365.0)
    for col in ("eps_surprise_pct", "revenue_growth_qoq_pct", "revenue_growth_yoy_pct", "net_margin_pct"):
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["eps_surprise_positive"] = pd.to_numeric(out["eps_surprise_positive"], errors="coerce").fillna(0.0)
    for col in default.columns:
        if col not in out.columns:
            out[col] = default[col]
    return out[default.columns]


def _build_daily_backtest_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], utc=True, errors="coerce")
    for col in ("Open", "High", "Low", "Close", "Volume"):
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=["date", "Open", "High", "Low", "Close"]).sort_values("date").reset_index(drop=True)
    if work.empty:
        return pd.DataFrame()

    work["rsi14"] = _compute_rsi_series_pd(work["Close"], 14)
    work["macd"], work["macd_signal"], work["macd_hist"] = _compute_macd_pd(work["Close"])
    work["ma50"] = work["Close"].rolling(window=50, min_periods=50).mean()
    work["ma200"] = work["Close"].rolling(window=200, min_periods=200).mean()
    work["ma20"] = work["Close"].rolling(window=20, min_periods=20).mean()
    atr, adx = _compute_atr_adx_pd(work, 14)
    work["atr14"] = atr
    work["adx14"] = adx
    work["atr_pct_14"] = (work["atr14"] / work["Close"]) * 100.0
    work["rvol20"] = work["Volume"] / work["Volume"].rolling(window=20, min_periods=20).mean()
    work["ret1"] = work["Close"].pct_change(1)
    work["ret3"] = work["Close"].pct_change(3)
    work["ret5"] = work["Close"].pct_change(5)
    work["ret10"] = work["Close"].pct_change(10)
    work["volume_ret1"] = work["Volume"].pct_change(1)
    work["price_vs_ma50"] = (work["Close"] / work["ma50"]) - 1.0
    work["price_vs_ma200"] = (work["Close"] / work["ma200"]) - 1.0
    work["dist_ma_20"] = (work["Close"] / work["ma20"]) - 1.0
    work["ma50_slope_5"] = work["ma50"].pct_change(5)
    work["ma200_slope_10"] = work["ma200"].pct_change(10)
    work["volatility_10"] = work["ret1"].rolling(window=10, min_periods=10).std()
    work["volatility_20"] = work["ret1"].rolling(window=20, min_periods=20).std()
    work["gap_prev_close"] = (work["Open"] / work["Close"].shift(1)) - 1.0
    work["range_pct"] = (work["High"] - work["Low"]) / work["Close"].replace(0.0, pd.NA)
    work["body_pct"] = (work["Close"] - work["Open"]).abs() / work["Close"].replace(0.0, pd.NA)
    work["close_location"] = (work["Close"] - work["Low"]) / (work["High"] - work["Low"]).replace(0.0, pd.NA)
    regime_parts = work.apply(_build_regime_score_from_row, axis=1, result_type="expand")
    if not regime_parts.empty:
        work["regime_score"] = regime_parts[0]
        work["regime_confidence"] = regime_parts[1]
    else:
        work["regime_score"] = pd.NA
        work["regime_confidence"] = pd.NA
    work["next_return_pct"] = ((work["Close"].shift(-1) / work["Close"]) - 1.0) * 100.0
    work["target_up"] = (work["Close"].shift(-1) > work["Close"]).astype("Int64")
    return work


def _daily_model_feature_cols() -> list[str]:
    return [
        "ret1",
        "ret3",
        "ret5",
        "ret10",
        "volume_ret1",
        "rsi14",
        "macd",
        "macd_signal",
        "macd_hist",
        "price_vs_ma50",
        "price_vs_ma200",
        "ma50_slope_5",
        "ma200_slope_10",
        "adx14",
        "atr_pct_14",
        "rvol20",
        "range_pct",
        "body_pct",
        "regime_score",
        "regime_confidence",
        "rel_spy_ret1",
        "rel_spy_ret5",
        "rel_qqq_ret1",
        "rel_qqq_ret5",
        "volatility_10",
        "volatility_20",
        "gap_prev_close",
        "close_location",
    ]


def _build_model_backtest_frame(work: pd.DataFrame, include_target: bool = True) -> tuple[pd.DataFrame, list[str]]:
    feature_cols = _daily_model_feature_cols()
    cols = ["date", "Close", *feature_cols]
    required = list(feature_cols)
    if include_target:
        cols[2:2] = ["next_return_pct", "target_up"]
        required += ["target_up", "next_return_pct"]
    model_df = work[cols].copy()
    model_df = model_df.dropna(subset=required).reset_index(drop=True)
    if include_target and not model_df.empty:
        model_df["target_up"] = model_df["target_up"].astype(int)
    return model_df, feature_cols


def _build_backtest_benchmark_returns(symbol: str) -> pd.DataFrame:
    source = _get_market_data_source()
    df, _ = _fetch_price_history_cached(
        ticker=symbol,
        period="3y",
        interval="1d",
        prepost=False,
        source=source,
        ttl_sec=24 * 3600,
        stale_ttl_sec=7 * 24 * 3600,
    )
    work = _build_daily_backtest_frame(df)
    if work.empty:
        return pd.DataFrame(columns=["date"])
    prefix = symbol.lower().replace(".", "_")
    return work[["date", "ret1", "ret5"]].rename(
        columns={
            "ret1": f"{prefix}_ret1",
            "ret5": f"{prefix}_ret5",
        }
    )


def _prepare_ticker_model_work(ticker: str) -> tuple[pd.DataFrame, str]:
    source = _get_market_data_source()
    df, provider_used = _fetch_price_history_cached(
        ticker=ticker,
        period="3y",
        interval="1d",
        prepost=False,
        source=source,
        ttl_sec=24 * 3600,
        stale_ttl_sec=7 * 24 * 3600,
    )
    work = _build_daily_backtest_frame(df)
    if work.empty or len(work.index) < 260:
        raise ValueError("Not enough daily history for backtest.")

    for benchmark_symbol in ("SPY", "QQQ"):
        bench = _build_backtest_benchmark_returns(benchmark_symbol)
        if bench.empty:
            raise ValueError(f"Benchmark {benchmark_symbol} history unavailable for backtest.")
        bench_prefix = benchmark_symbol.lower().replace(".", "_")
        work = work.merge(bench, on="date", how="left")
        work[f"rel_{bench_prefix}_ret1"] = work["ret1"] - work[f"{bench_prefix}_ret1"]
        work[f"rel_{bench_prefix}_ret5"] = work["ret5"] - work[f"{bench_prefix}_ret5"]
    return work, provider_used


def _prepare_ticker_model_frame(ticker: str) -> tuple[pd.DataFrame, str, list[str]]:
    work, provider_used = _prepare_ticker_model_work(ticker)
    model_df, feature_cols = _build_model_backtest_frame(work)
    if model_df.empty or len(model_df.index) < 300:
        raise ValueError("Not enough modeled daily history for backtest.")

    n = len(model_df.index)
    train_end = max(180, int(n * 0.60))
    val_end = max(train_end + 40, int(n * 0.70))
    val_end = min(val_end, n - 60)
    if train_end < 180 or val_end <= train_end or (n - val_end) < 40:
        raise ValueError("Not enough train/validation/test history for model backtest.")

    model_df = model_df.copy()
    model_df["ticker"] = ticker
    model_df["provider_used"] = provider_used
    parts = ["test"] * n
    for i in range(train_end):
        parts[i] = "train"
    for i in range(train_end, val_end):
        parts[i] = "val"
    model_df["part"] = parts
    return model_df, provider_used, feature_cols


def _prepare_ticker_scan_frame(ticker: str) -> tuple[pd.DataFrame, pd.Series, str, list[str]]:
    work, provider_used = _prepare_ticker_model_work(ticker)
    trainable_df, feature_cols = _build_model_backtest_frame(work, include_target=True)
    live_df, _ = _build_model_backtest_frame(work, include_target=False)
    if trainable_df.empty or len(trainable_df.index) < 300:
        raise ValueError("Not enough modeled daily history for scanner.")
    if live_df.empty:
        raise ValueError("No current feature row available for scanner.")
    live_row = live_df.iloc[-1].copy()
    return trainable_df, live_row, provider_used, feature_cols


def _run_pooled_backtest(
    tickers: list[str],
    prob_threshold: float = 0.90,
    confidence_threshold: float = 75.0,
) -> tuple[list[dict], dict, list[str]]:
    try:
        from sklearn.isotonic import IsotonicRegression
        from sklearn.metrics import accuracy_score
        from lightgbm import LGBMClassifier
    except Exception as exc:
        raise RuntimeError(
            "lightgbm and scikit-learn are required for model backtest. Install them in the app environment."
        ) from exc

    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    provider_used = _get_market_data_source()
    feature_cols: list[str] | None = None
    for idx, ticker in enumerate(tickers):
        try:
            model_df, provider_used, feature_cols = _prepare_ticker_model_frame(ticker)
            frames.append(model_df)
        except Exception as exc:
            errors.append(f"{ticker}: {exc}")
        if idx < len(tickers) - 1:
            time.sleep(0.8)

    if not frames or not feature_cols:
        raise ValueError("Backtest failed for all tickers.")

    all_df = pd.concat(frames, ignore_index=True)
    train_df = all_df[all_df["part"] == "train"].copy()
    val_df = all_df[all_df["part"] == "val"].copy()
    test_df = all_df[all_df["part"] == "test"].copy()
    if train_df.empty or val_df.empty or test_df.empty:
        raise ValueError("Not enough pooled train/validation/test rows for backtest.")

    model = LGBMClassifier(
        objective="binary",
        n_estimators=500,
        max_depth=5,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2.0,
        min_child_samples=40,
        num_leaves=31,
        random_state=42,
        n_jobs=4,
        verbosity=-1,
    )
    model.fit(train_df[feature_cols], train_df["target_up"].astype(int))

    val_probs = model.predict_proba(val_df[feature_cols])[:, 1]
    calibrator = IsotonicRegression(out_of_bounds="clip")
    calibrator.fit(val_probs, val_df["target_up"].astype(int))

    raw_test_probs = model.predict_proba(test_df[feature_cols])[:, 1]
    test_probs = calibrator.transform(raw_test_probs)
    test_df = test_df.copy()
    test_df["p_up_tomorrow"] = test_probs
    test_df["p_down_tomorrow"] = 1.0 - test_probs
    test_df["confidence_pct"] = (
        pd.Series(test_probs, index=test_df.index).sub(0.5).abs().mul(200.0).clip(lower=0.0, upper=100.0)
    )
    test_df["predicted_up"] = test_df["p_up_tomorrow"] >= 0.5
    test_df["actual_up"] = test_df["target_up"].astype(int) == 1
    test_df["actual_return_pct"] = pd.to_numeric(test_df["next_return_pct"], errors="coerce")

    baseline_always_up = (test_df["actual_up"] == True).mean() * 100.0
    baseline_prev_momentum = (
        (pd.to_numeric(test_df["ret1"], errors="coerce").fillna(0.0) > 0.0) == test_df["actual_up"]
    ).mean() * 100.0
    baseline_mean_revert = (
        (pd.to_numeric(test_df["ret1"], errors="coerce").fillna(0.0) <= 0.0) == test_df["actual_up"]
    ).mean() * 100.0

    bucket_specs = [
        ("50-60%", 0.50, 0.60),
        ("60-70%", 0.60, 0.70),
        ("70-80%", 0.70, 0.80),
        ("80-90%", 0.80, 0.90),
        ("90%+", 0.90, 1.01),
    ]
    aggregate_buckets: list[dict] = []
    strongest = test_df[["p_up_tomorrow", "p_down_tomorrow"]].max(axis=1)
    for label, low, high in bucket_specs:
        mask = (strongest >= low) & (strongest < high)
        count = int(mask.sum())
        hit_rate = None
        if count:
            hit_rate = float(round((test_df.loc[mask, "predicted_up"] == test_df.loc[mask, "actual_up"]).mean() * 100.0, 1))
        aggregate_buckets.append({"label": label, "count": count, "hit_rate": hit_rate})

    threshold_sweep: list[dict] = []
    candidate_prob_thresholds = [0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90]
    candidate_conf_thresholds = [50.0, 60.0, 70.0, 75.0, 80.0]
    for p_thresh in candidate_prob_thresholds:
        for conf_thresh in candidate_conf_thresholds:
            mask = (
                (test_df[["p_up_tomorrow", "p_down_tomorrow"]].max(axis=1) >= p_thresh)
                & (test_df["confidence_pct"] >= conf_thresh)
            )
            count = int(mask.sum())
            if not count:
                threshold_sweep.append(
                    {
                        "prob_threshold": p_thresh,
                        "confidence_threshold": conf_thresh,
                        "signals": 0,
                        "hit_rate": None,
                    }
                )
                continue
            hit_rate = round((test_df.loc[mask, "predicted_up"] == test_df.loc[mask, "actual_up"]).mean() * 100.0, 1)
            threshold_sweep.append(
                {
                    "prob_threshold": p_thresh,
                    "confidence_threshold": conf_thresh,
                    "signals": count,
                    "hit_rate": hit_rate,
                }
            )
    recommended_thresholds = None
    viable = [
        item for item in threshold_sweep
        if item.get("signals", 0) >= 20 and item.get("hit_rate") is not None
    ]
    if viable:
        viable.sort(
            key=lambda item: (
                float(item.get("hit_rate") or 0.0),
                math.log10(max(1, int(item.get("signals") or 1))),
                float(item.get("prob_threshold") or 0.0),
                -float(item.get("confidence_threshold") or 0.0),
            ),
            reverse=True,
        )
        recommended_thresholds = viable[0]

    results: list[dict] = []
    for ticker, ticker_df in test_df.groupby("ticker", sort=True):
        tested = int(len(ticker_df.index))
        overall_hit_rate = round((ticker_df["predicted_up"] == ticker_df["actual_up"]).mean() * 100.0, 1) if tested else None
        strong_up_mask = (ticker_df["p_up_tomorrow"] >= prob_threshold) & (ticker_df["confidence_pct"] >= confidence_threshold)
        strong_down_mask = (ticker_df["p_down_tomorrow"] >= prob_threshold) & (ticker_df["confidence_pct"] >= confidence_threshold)
        strong_up_total = int(strong_up_mask.sum())
        strong_down_total = int(strong_down_mask.sum())
        strong_up_hit_rate = round((ticker_df.loc[strong_up_mask, "actual_up"]).mean() * 100.0, 1) if strong_up_total else None
        strong_down_hit_rate = round((~ticker_df.loc[strong_down_mask, "actual_up"]).mean() * 100.0, 1) if strong_down_total else None
        recent_signal_rows = ticker_df.loc[strong_up_mask | strong_down_mask].tail(8)
        recent_signals = [
            {
                "date": pd.to_datetime(row["date"], utc=True).date().isoformat(),
                "bias": "bullish" if float(row["p_up_tomorrow"]) >= float(row["p_down_tomorrow"]) else "bearish",
                "p_up_tomorrow": round(float(row["p_up_tomorrow"]), 4),
                "p_down_tomorrow": round(float(row["p_down_tomorrow"]), 4),
                "confidence_pct": round(float(row["confidence_pct"]), 1),
                "actual_return_pct": round(float(row["actual_return_pct"]), 3) if pd.notna(row["actual_return_pct"]) else None,
                "actual_direction": "up" if bool(row["actual_up"]) else "down",
            }
            for _, row in recent_signal_rows.iterrows()
        ]
        results.append(
            {
                "ticker": ticker,
                "provider_used": provider_used,
                "days_tested": tested,
                "overall_hit_rate": overall_hit_rate,
                "strong_up_signals": strong_up_total,
                "strong_up_hit_rate": strong_up_hit_rate,
                "strong_down_signals": strong_down_total,
                "strong_down_hit_rate": strong_down_hit_rate,
                "thresholds": {
                    "p_threshold": prob_threshold,
                    "confidence_threshold": confidence_threshold,
                },
                "recent_strong_signals": recent_signals,
                "notes": [
                    "Uses a pooled daily model trained across the selected ticker universe.",
                    "Probabilities are calibrated on a chronological validation split.",
                    "Backtest intentionally omits archived news/fundamentals to avoid look-ahead bias.",
                ],
            }
        )

    summary = {
        "tickers_tested": len(results),
        "overall_hit_rate": round(accuracy_score(test_df["actual_up"], test_df["predicted_up"]) * 100.0, 1),
        "strong_up_signals": int(((test_df["p_up_tomorrow"] >= prob_threshold) & (test_df["confidence_pct"] >= confidence_threshold)).sum()),
        "strong_down_signals": int(((test_df["p_down_tomorrow"] >= prob_threshold) & (test_df["confidence_pct"] >= confidence_threshold)).sum()),
        "model": {
            "type": "LightGBMClassifier + IsotonicRegression",
            "train_days": int(len(train_df.index)),
            "validation_days": int(len(val_df.index)),
            "test_days": int(len(test_df.index)),
            "features": feature_cols,
            "params": {
                "objective": "high-confidence signal precision",
                "n_estimators": 500,
                "max_depth": 5,
                "learning_rate": 0.03,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "reg_lambda": 2.0,
                "min_child_samples": 40,
                "num_leaves": 31,
                "split": "chronological 60/10/30 per ticker, pooled across selected tickers",
            },
        },
        "calibration_buckets": aggregate_buckets,
        "baselines": [
            {"name": "Always Up", "hit_rate": float(round(baseline_always_up, 1))},
            {"name": "Prev-Day Momentum", "hit_rate": float(round(baseline_prev_momentum, 1))},
            {"name": "One-Day Mean Reversion", "hit_rate": float(round(baseline_mean_revert, 1))},
        ],
        "threshold_sweep": threshold_sweep,
        "recommended_thresholds": recommended_thresholds,
        "notes": [
            "Backtest uses a pooled model across the selected ticker universe.",
            "The model is selected for stronger high-confidence alert precision, not peak overall hit rate.",
            "Higher probability buckets should outperform lower buckets after calibration.",
            "Archived news/fundamentals are excluded to avoid look-ahead bias.",
        ],
    }
    return results, summary, errors


def _make_calibrated_daily_model():
    from sklearn.isotonic import IsotonicRegression
    from lightgbm import LGBMClassifier

    model = LGBMClassifier(
        objective="binary",
        n_estimators=500,
        max_depth=5,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2.0,
        min_child_samples=40,
        num_leaves=31,
        random_state=42,
        n_jobs=4,
        verbosity=-1,
    )
    calibrator = IsotonicRegression(out_of_bounds="clip")
    return model, calibrator


def _scanner_model_artifact_path(tickers: list[str]) -> Path:
    normalized = [symbol for symbol in sorted({_sanitize_ticker(t) for t in tickers}) if symbol]
    universe_key = "|".join(normalized)
    digest = hashlib.sha256(f"{DAILY_SCANNER_MODEL_VERSION}|{universe_key}".encode("utf-8")).hexdigest()[:16]
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    return MODEL_DIR / f"scanner_model_{digest}.pkl"


def _load_scanner_model_artifact(tickers: list[str]) -> dict | None:
    path = _scanner_model_artifact_path(tickers)
    if not path.exists():
        return None
    try:
        with path.open("rb") as fh:
            artifact = pickle.load(fh)
        if not isinstance(artifact, dict):
            return None
        if artifact.get("version") != DAILY_SCANNER_MODEL_VERSION:
            return None
        stored = artifact.get("tickers")
        current = sorted({_sanitize_ticker(t) for t in tickers if _sanitize_ticker(t)})
        if stored != current:
            return None
        if not artifact.get("feature_cols") or artifact.get("model") is None or artifact.get("calibrator") is None:
            return None
        return artifact
    except Exception:
        return None


def _save_scanner_model_artifact(tickers: list[str], artifact: dict) -> None:
    path = _scanner_model_artifact_path(tickers)
    payload = {
        "version": DAILY_SCANNER_MODEL_VERSION,
        "tickers": sorted({_sanitize_ticker(t) for t in tickers if _sanitize_ticker(t)}),
        "trained_at_utc": datetime.now(timezone.utc).isoformat(),
        "feature_cols": list(artifact.get("feature_cols") or []),
        "model": artifact.get("model"),
        "calibrator": artifact.get("calibrator"),
        "train_rows": int(artifact.get("train_rows") or 0),
        "validation_rows": int(artifact.get("validation_rows") or 0),
    }
    with path.open("wb") as fh:
        pickle.dump(payload, fh, protocol=pickle.HIGHEST_PROTOCOL)


def _run_live_scanner(
    tickers: list[str],
    prob_threshold: float = 0.80,
    confidence_threshold: float = 50.0,
    force_retrain: bool = False,
) -> tuple[list[dict], dict, list[str]]:
    try:
        _, _ = _make_calibrated_daily_model()
    except Exception as exc:
        raise RuntimeError(
            "lightgbm and scikit-learn are required for scanner. Install them in the app environment."
        ) from exc

    artifact = None if force_retrain else _load_scanner_model_artifact(tickers)
    train_frames: list[pd.DataFrame] = []
    val_frames: list[pd.DataFrame] = []
    live_rows: list[dict] = []
    errors: list[str] = []
    feature_cols: list[str] | None = list(artifact.get("feature_cols") or []) if artifact else None
    provider_used = _get_market_data_source()
    for idx, ticker in enumerate(tickers):
        try:
            trainable_df, live_row, provider_used, feature_cols = _prepare_ticker_scan_frame(ticker)
            if artifact is None:
                n = len(trainable_df.index)
                val_start = max(220, int(n * 0.90))
                val_start = min(val_start, n - 40)
                if val_start < 180 or (n - val_start) < 30:
                    raise ValueError("Not enough train/validation rows for scanner.")
                train_frames.append(trainable_df.iloc[:val_start].copy())
                val_frames.append(trainable_df.iloc[val_start:].copy())
            live_rows.append(
                {
                    "ticker": ticker,
                    "provider_used": provider_used,
                    "date": live_row.get("date"),
                    "close": _float_or_none(live_row.get("Close")),
                    "features": live_row[feature_cols].copy(),
                }
            )
        except Exception as exc:
            errors.append(f"{ticker}: {exc}")
        if idx < len(tickers) - 1:
            time.sleep(0.8)

    if not live_rows or not feature_cols:
        raise ValueError("Scanner failed for all tickers.")
    if artifact is None:
        if not train_frames or not val_frames:
            raise ValueError("Scanner failed for all tickers.")
        train_df = pd.concat(train_frames, ignore_index=True)
        val_df = pd.concat(val_frames, ignore_index=True)
        model, calibrator = _make_calibrated_daily_model()
        model.fit(train_df[feature_cols], train_df["target_up"].astype(int))
        val_probs = model.predict_proba(val_df[feature_cols])[:, 1]
        calibrator.fit(val_probs, val_df["target_up"].astype(int))
        _save_scanner_model_artifact(
            tickers,
            {
                "feature_cols": feature_cols,
                "model": model,
                "calibrator": calibrator,
                "train_rows": len(train_df.index),
                "validation_rows": len(val_df.index),
            },
        )
        model_source = "trained_now"
        train_rows = int(len(train_df.index))
        validation_rows = int(len(val_df.index))
        trained_at = datetime.now(timezone.utc).isoformat()
    else:
        model = artifact["model"]
        calibrator = artifact["calibrator"]
        model_source = "disk_cache"
        train_rows = int(artifact.get("train_rows") or 0)
        validation_rows = int(artifact.get("validation_rows") or 0)
        trained_at = str(artifact.get("trained_at_utc") or "")

    signals: list[dict] = []
    for item in live_rows:
        features_df = pd.DataFrame([item["features"]], columns=feature_cols)
        raw_prob = float(model.predict_proba(features_df)[0, 1])
        p_up = float(calibrator.transform([raw_prob])[0])
        p_down = 1.0 - p_up
        confidence_pct = max(0.0, min(100.0, abs(p_up - 0.5) * 200.0))
        strongest = max(p_up, p_down)
        if strongest < prob_threshold or confidence_pct < confidence_threshold:
            continue
        bias = "bullish" if p_up >= p_down else "bearish"
        signals.append(
            {
                "ticker": item["ticker"],
                "provider_used": item["provider_used"],
                "as_of": pd.to_datetime(item["date"], utc=True).isoformat() if item.get("date") is not None else "",
                "close": round(float(item["close"]), 4) if item.get("close") is not None else None,
                "bias": bias,
                "p_up_tomorrow": round(p_up, 4),
                "p_down_tomorrow": round(p_down, 4),
                "confidence_pct": round(confidence_pct, 1),
            }
        )

    signals.sort(
        key=lambda item: (
            float(max(item.get("p_up_tomorrow", 0.0), item.get("p_down_tomorrow", 0.0))),
            float(item.get("confidence_pct", 0.0)),
        ),
        reverse=True,
    )
    summary = {
        "tickers_considered": len(live_rows),
        "signals_found": len(signals),
        "mode": "more_signals",
        "thresholds": {
            "prob_threshold": prob_threshold,
            "confidence_threshold": confidence_threshold,
        },
        "model": {
            "type": "LightGBMClassifier + IsotonicRegression",
            "objective": "high-confidence signal precision",
            "features": feature_cols,
            "source": model_source,
            "train_rows": train_rows,
            "validation_rows": validation_rows,
            "trained_at_utc": trained_at,
        },
    }
    return signals, summary, errors


def _build_backtest_metrics_from_frame(work: pd.DataFrame, idx: int, lookback: int = 20) -> dict | None:
    if idx <= 0 or idx >= len(work.index):
        return None
    row = work.iloc[idx]
    start_idx = max(0, idx - lookback + 1)
    window = work.iloc[start_idx : idx + 1]
    if window.empty:
        return None

    start_close = _float_or_none(window.iloc[0].get("Close"))
    end_close = _float_or_none(row.get("Close"))
    if start_close is None or end_close is None or start_close <= 0:
        return None
    change_pct = ((end_close / start_close) - 1.0) * 100.0
    high = pd.to_numeric(window["High"], errors="coerce").max()
    low = pd.to_numeric(window["Low"], errors="coerce").min()
    avg_volume = pd.to_numeric(window["Volume"], errors="coerce").mean()

    return {
        "summary": {
            "start": round(start_close, 4),
            "end": round(end_close, 4),
            "change_pct": round(change_pct, 3),
            "high": round(float(high), 4) if pd.notna(high) else None,
            "low": round(float(low), 4) if pd.notna(low) else None,
            "avg_volume": round(float(avg_volume), 0) if pd.notna(avg_volume) else None,
            "last_volume": round(float(_float_or_none(row.get("Volume")) or 0.0), 0),
        },
        "indicators": {
            "rsi14": _float_or_none(row.get("rsi14")),
            "macd": _float_or_none(row.get("macd")),
            "macd_signal": _float_or_none(row.get("macd_signal")),
            "macd_hist": _float_or_none(row.get("macd_hist")),
            "ma50": _float_or_none(row.get("ma50")),
            "ma200": _float_or_none(row.get("ma200")),
            "adx14": _float_or_none(row.get("adx14")),
            "atr_pct_14": _float_or_none(row.get("atr_pct_14")),
            "rvol20": _float_or_none(row.get("rvol20")),
            "regime_score": _float_or_none(row.get("regime_score")),
            "regime_confidence": _float_or_none(row.get("regime_confidence")),
        },
        "patterns": [],
    }


def _run_ticker_backtest(
    ticker: str,
    prob_threshold: float = 0.90,
    confidence_threshold: float = 75.0,
) -> dict:
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
    except Exception as exc:
        raise RuntimeError("scikit-learn is required for model backtest. Install it in the app environment.") from exc

    df, provider_used = _fetch_price_history_cached(
        ticker=ticker,
        period="3y",
        interval="1d",
        prepost=False,
        source=_get_market_data_source(),
        ttl_sec=24 * 3600,
        stale_ttl_sec=7 * 24 * 3600,
    )
    work = _build_daily_backtest_frame(df)
    if work.empty or len(work.index) < 260:
        raise ValueError("Not enough daily history for backtest.")
    model_df, feature_cols = _build_model_backtest_frame(work)
    if model_df.empty or len(model_df.index) < 260:
        raise ValueError("Not enough modeled daily history for backtest.")

    split_idx = max(200, int(len(model_df.index) * 0.70))
    split_idx = min(split_idx, len(model_df.index) - 60)
    if split_idx < 200 or (len(model_df.index) - split_idx) < 40:
        raise ValueError("Not enough train/test history for model backtest.")

    train_df = model_df.iloc[:split_idx].copy()
    test_df = model_df.iloc[split_idx:].copy()
    X_train = train_df[feature_cols]
    y_train = train_df["target_up"]
    X_test = test_df[feature_cols]
    y_test = test_df["target_up"]

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("logreg", LogisticRegression(max_iter=1500, C=0.5, class_weight="balanced")),
        ]
    )
    model.fit(X_train, y_train)
    probs_up = model.predict_proba(X_test)[:, 1]
    preds_up = probs_up >= 0.5

    tested = 0
    directional_hits = 0
    strong_up_total = 0
    strong_up_hits = 0
    strong_down_total = 0
    strong_down_hits = 0
    strong_signals: list[dict] = []
    calibration_buckets = [
        {"label": "50-60%", "low": 0.50, "high": 0.60, "count": 0, "hits": 0},
        {"label": "60-70%", "low": 0.60, "high": 0.70, "count": 0, "hits": 0},
        {"label": "70-80%", "low": 0.70, "high": 0.80, "count": 0, "hits": 0},
        {"label": "80-90%", "low": 0.80, "high": 0.90, "count": 0, "hits": 0},
        {"label": "90%+", "low": 0.90, "high": 1.01, "count": 0, "hits": 0},
    ]

    for local_idx, (_, row) in enumerate(test_df.iterrows()):
        next_return_pct = _float_or_none(row.get("next_return_pct"))
        close_now = _float_or_none(row.get("Close"))
        if close_now is None or close_now <= 0:
            continue
        tested += 1
        actual_return_pct = float(next_return_pct or 0.0)
        actual_up = actual_return_pct > 0
        p_up = float(probs_up[local_idx])
        p_down = 1.0 - p_up
        predicted_up = bool(preds_up[local_idx])
        if predicted_up == actual_up:
            directional_hits += 1

        confidence = _clamp(abs(p_up - 0.5) * 200.0, 0.0, 100.0)
        strongest_prob = max(p_up, p_down)

        for bucket in calibration_buckets:
            if bucket["low"] <= strongest_prob < bucket["high"]:
                bucket["count"] += 1
                predicted_direction_up = p_up >= p_down
                if (predicted_direction_up and actual_up) or ((not predicted_direction_up) and (not actual_up)):
                    bucket["hits"] += 1
                break

        signal_record = {
            "date": pd.to_datetime(row["date"], utc=True).date().isoformat(),
            "bias": "bullish" if p_up >= p_down else "bearish",
            "p_up_tomorrow": round(p_up, 4),
            "p_down_tomorrow": round(p_down, 4),
            "confidence_pct": round(confidence, 1),
            "actual_return_pct": round(actual_return_pct, 3),
            "actual_direction": "up" if actual_up else "down",
        }

        if p_up >= prob_threshold and confidence >= confidence_threshold:
            strong_up_total += 1
            if actual_up:
                strong_up_hits += 1
            strong_signals.append(signal_record)
        elif p_down >= prob_threshold and confidence >= confidence_threshold:
            strong_down_total += 1
            if not actual_up:
                strong_down_hits += 1
            strong_signals.append(signal_record)

    if tested == 0:
        raise ValueError("No eligible daily bars for backtest.")

    return {
        "ticker": ticker,
        "provider_used": provider_used,
        "days_tested": tested,
        "overall_hit_rate": round((directional_hits / tested) * 100.0, 1),
        "strong_up_signals": strong_up_total,
        "strong_up_hit_rate": round((strong_up_hits / strong_up_total) * 100.0, 1) if strong_up_total else None,
        "strong_down_signals": strong_down_total,
        "strong_down_hit_rate": round((strong_down_hits / strong_down_total) * 100.0, 1) if strong_down_total else None,
        "thresholds": {
            "p_threshold": prob_threshold,
            "confidence_threshold": confidence_threshold,
        },
        "model": {
            "type": "LogisticRegression",
            "train_days": int(len(train_df.index)),
            "test_days": int(len(test_df.index)),
            "features": feature_cols,
            "params": {
                "C": 0.5,
                "class_weight": "balanced",
                "max_iter": 1500,
                "split": "chronological 70/30",
            },
        },
        "calibration_buckets": [
            {
                "label": bucket["label"],
                "count": int(bucket["count"]),
                "hit_rate": round((bucket["hits"] / bucket["count"]) * 100.0, 1) if bucket["count"] else None,
            }
            for bucket in calibration_buckets
        ],
        "recent_strong_signals": strong_signals[-8:],
        "notes": [
            "Uses each ticker's own daily history only; no cross-ticker mixing.",
            "Uses a per-ticker logistic regression trained on daily technical features.",
            "Backtest intentionally omits archived news/fundamentals to avoid look-ahead bias.",
        ],
    }


def _fetch_finnhub_news_cached(ticker: str, day_window: int = 10, ttl_sec: int = 300) -> list[dict]:
    symbol = _sanitize_ticker(ticker)
    if not symbol:
        raise ValueError("Ticker is required.")

    api_key = _get_config_value("FINNHUB_API_KEY")
    if not api_key:
        raise ValueError("FINNHUB_API_KEY is not configured.")

    now = time.time()
    with _news_cache_lock:
        hit = _news_cache.get(symbol)
        if hit and (now - float(hit.get("at", 0.0))) < ttl_sec:
            cached_items = hit.get("items")
            if isinstance(cached_items, list):
                return list(cached_items)

    end_dt = datetime.now(timezone.utc).date()
    start_dt = end_dt - timedelta(days=max(2, int(day_window)))
    params = urllib.parse.urlencode(
        {
            "symbol": symbol,
            "from": start_dt.isoformat(),
            "to": end_dt.isoformat(),
            "token": api_key,
        }
    )
    url = f"https://finnhub.io/api/v1/company-news?{params}"

    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        msg = body.strip() or str(exc)
        raise ValueError(f"Finnhub HTTP {exc.code}: {msg}") from exc
    except urllib.error.URLError as exc:
        raise ValueError(f"Finnhub request failed: {exc.reason}") from exc

    if not isinstance(payload, list):
        raise ValueError("Finnhub returned an unexpected response.")

    items: list[dict] = []
    for raw in payload[:25]:
        if not isinstance(raw, dict):
            continue
        headline = str(raw.get("headline") or "").strip()
        url = str(raw.get("url") or "").strip()
        if not headline or not url:
            continue
        ts = raw.get("datetime")
        try:
            published_at = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat() if ts else None
        except Exception:
            published_at = None
        items.append(
            {
                "headline": headline,
                "source": str(raw.get("source") or "").strip(),
                "summary": str(raw.get("summary") or "").strip(),
                "url": url,
                "published_at": published_at,
                "image": str(raw.get("image") or "").strip(),
            }
        )

    with _news_cache_lock:
        _news_cache[symbol] = {"at": now, "items": list(items)}
    return items


def _ensure_portfolio_storage() -> None:
    PORTFOLIO_FILES_DIR.mkdir(parents=True, exist_ok=True)


def _ensure_log_storage() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def _ensure_category_storage() -> None:
    CATEGORY_DIR.mkdir(parents=True, exist_ok=True)


def _ensure_watchlist_cache_storage() -> None:
    WATCHLIST_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _setup_logging() -> None:
    _ensure_log_storage()
    if any(
        isinstance(h, logging.FileHandler) and Path(getattr(h, "baseFilename", "")) == SERVER_LOG_PATH
        for h in app.logger.handlers
    ):
        return

    file_handler = logging.FileHandler(SERVER_LOG_PATH, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))

    app.logger.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)


def _append_client_action_log(entry: dict) -> None:
    _ensure_log_storage()
    with CLIENT_ACTION_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=True) + "\n")


def _append_upstream_api_log(entry: dict) -> None:
    _ensure_log_storage()
    with UPSTREAM_API_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=True) + "\n")


@contextlib.contextmanager
def _upstream_usage_scope(label: str):
    token = set_upstream_usage_context(label)
    try:
        yield
    finally:
        reset_upstream_usage_context(token)


def _read_recent_jsonl(path: Path, max_entries: int = 500) -> list[dict]:
    rows: deque[dict] = deque(maxlen=max_entries)
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
    except Exception:
        return []
    return list(rows)


def _build_upstream_usage_snapshot(max_entries: int = 500) -> dict:
    entries = _read_recent_jsonl(UPSTREAM_API_LOG_PATH, max_entries=max_entries)
    today_key = _current_local_day_key()
    by_context: dict[str, dict] = {}
    by_provider: dict[str, int] = {}
    by_endpoint: dict[str, int] = {}
    total = 0
    rate_limited = 0
    error_count = 0
    filtered_entries: list[dict] = []

    for entry in entries:
        ts_utc = str(entry.get("ts_utc") or "").strip()
        if not ts_utc:
            continue
        try:
            ts_local_day = datetime.fromisoformat(ts_utc).astimezone().date().isoformat()
        except Exception:
            continue
        if ts_local_day != today_key:
            continue
        filtered_entries.append(entry)
        total += 1
        context = str(entry.get("usage_context") or "other").strip().lower() or "other"
        provider = str(entry.get("provider") or "unknown").strip().lower() or "unknown"
        endpoint = str(entry.get("endpoint") or "unknown").strip() or "unknown"
        status = entry.get("http_status")
        status_int = int(status) if isinstance(status, int) or (isinstance(status, str) and status.isdigit()) else None
        is_rate_limited = status_int == 429
        is_error = (status_int is not None and status_int >= 400) or bool(entry.get("error"))

        bucket = by_context.setdefault(
            context,
            {"context": context, "calls": 0, "rate_limited": 0, "errors": 0, "last_call_utc": "", "providers": {}},
        )
        bucket["calls"] += 1
        if is_rate_limited:
            bucket["rate_limited"] += 1
            rate_limited += 1
        if is_error:
            bucket["errors"] += 1
            error_count += 1
        if ts_utc and ts_utc > str(bucket.get("last_call_utc") or ""):
            bucket["last_call_utc"] = ts_utc
        provider_counts = bucket["providers"]
        provider_counts[provider] = int(provider_counts.get(provider) or 0) + 1

        by_provider[provider] = int(by_provider.get(provider) or 0) + 1
        endpoint_key = f"{provider}:{endpoint}"
        by_endpoint[endpoint_key] = int(by_endpoint.get(endpoint_key) or 0) + 1

    recent_entries: list[dict] = []
    for entry in reversed(filtered_entries[-200:]):
        recent_entries.append(
            {
                "ts_utc": str(entry.get("ts_utc") or ""),
                "usage_context": str(entry.get("usage_context") or "other"),
                "provider": str(entry.get("provider") or ""),
                "endpoint": str(entry.get("endpoint") or ""),
                "symbol": str(entry.get("symbol") or ""),
                "interval": str(entry.get("interval") or ""),
                "http_status": entry.get("http_status"),
                "diagnostic": bool(entry.get("diagnostic")),
                "error": str(entry.get("error") or ""),
            }
        )

    return {
        "day_key": today_key,
        "totals": {
            "calls": total,
            "rate_limited": rate_limited,
            "errors": error_count,
        },
        "by_context": sorted(by_context.values(), key=lambda item: (-int(item.get("calls") or 0), str(item.get("context") or ""))),
        "by_provider": [{"provider": key, "calls": by_provider[key]} for key in sorted(by_provider, key=lambda k: (-by_provider[k], k))],
        "by_endpoint": [{"endpoint": key, "calls": by_endpoint[key]} for key in sorted(by_endpoint, key=lambda k: (-by_endpoint[k], k))[:20]],
        "recent_entries": recent_entries,
    }


def _append_rsi_monitor_log(entry: dict) -> None:
    _ensure_log_storage()
    with RSI_MONITOR_LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=True) + "\n")


def _extract_watchlist_symbols_from_categories() -> list[str]:
    categories = _load_category_config()
    symbols: list[str] = []
    seen: set[str] = set()
    for cat in categories:
        if not isinstance(cat, dict):
            continue
        for it in cat.get("items", []) if isinstance(cat.get("items"), list) else []:
            if not isinstance(it, dict):
                continue
            sym = _sanitize_ticker(it.get("symbol"))
            if not sym or sym in seen:
                continue
            seen.add(sym)
            symbols.append(sym)
    return symbols


def _current_local_day_key() -> str:
    try:
        return datetime.now().astimezone().date().isoformat()
    except Exception:
        return datetime.now(timezone.utc).date().isoformat()


def _watchlist_cache_key(tickers: list[str], range_key: str) -> str:
    normalized = sorted({_sanitize_ticker(t) for t in tickers if _sanitize_ticker(t)})
    digest = hashlib.sha1((",".join(normalized)).encode("utf-8")).hexdigest()[:16]
    return f"{_current_local_day_key()}::{range_key}::{digest}"


def _extract_watchlist_error_ticker(message: str) -> str:
    text = str(message or "").strip()
    if ":" not in text:
        return ""
    return _sanitize_ticker(text.split(":", 1)[0])


def _copy_watchlist_entry(entry: dict | None) -> dict:
    if not isinstance(entry, dict):
        return {}
    return {
        "ok": bool(entry.get("ok", True)),
        "status": str(entry.get("status") or "pending"),
        "source": str(entry.get("source") or _get_market_data_source()),
        "day_key": str(entry.get("day_key") or ""),
        "universe_label": str(entry.get("universe_label") or ""),
        "range": str(entry.get("range") or "1d"),
        "tickers_considered": int(entry.get("tickers_considered") or 0),
        "loaded_count": int(entry.get("loaded_count") or 0),
        "items": list(entry.get("items") or []),
        "no_match_tickers": list(entry.get("no_match_tickers") or []),
        "errors": list(entry.get("errors") or []),
        "started_at": str(entry.get("started_at") or ""),
        "completed_at": str(entry.get("completed_at") or ""),
        "last_ticker": str(entry.get("last_ticker") or ""),
        "cache_key": str(entry.get("cache_key") or ""),
        "force_refresh": bool(entry.get("force_refresh", False)),
        "delay_sec": float(entry.get("delay_sec") or WATCHLIST_SCAN_DELAY_SEC),
        "scan_mode": str(entry.get("scan_mode") or "interactive"),
    }


def _watchlist_cache_file_path(cache_key: str) -> Path:
    _ensure_watchlist_cache_storage()
    safe_key = re.sub(r"[^A-Za-z0-9._:-]", "_", str(cache_key or "watchlist"))
    return WATCHLIST_CACHE_DIR / f"{safe_key}.json"


def _write_watchlist_cache_entry_to_disk(cache_key: str, entry: dict) -> None:
    try:
        path = _watchlist_cache_file_path(cache_key)
        path.write_text(json.dumps(_copy_watchlist_entry(entry), ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _read_watchlist_cache_entry_from_disk(cache_key: str) -> dict:
    try:
        path = _watchlist_cache_file_path(cache_key)
        if not path.exists():
            return {}
        entry = json.loads(path.read_text(encoding="utf-8"))
        copied = _copy_watchlist_entry(entry if isinstance(entry, dict) else {})
        if str(copied.get("day_key") or "") != _current_local_day_key():
            return {}
        if str(copied.get("status") or "") == "running":
            copied["status"] = "ready"
            copied["completed_at"] = str(copied.get("completed_at") or copied.get("started_at") or "")
        return copied
    except Exception:
        return {}


def _prune_watchlist_cache_locked() -> None:
    today_key = _current_local_day_key()
    stale_keys = [key for key, value in _watchlist_cache.items() if str((value or {}).get("day_key") or "") != today_key]
    for key in stale_keys:
        _watchlist_cache.pop(key, None)
    try:
        _ensure_watchlist_cache_storage()
        for path in WATCHLIST_CACHE_DIR.glob("*.json"):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                path.unlink(missing_ok=True)
                continue
            if str((payload or {}).get("day_key") or "") != today_key:
                path.unlink(missing_ok=True)
    except Exception:
        pass


def _set_watchlist_cache_entry(cache_key: str, entry: dict) -> None:
    with _watchlist_cache_lock:
        _prune_watchlist_cache_locked()
        copied = _copy_watchlist_entry(entry)
        _watchlist_cache[cache_key] = dict(copied)
    _write_watchlist_cache_entry_to_disk(cache_key, copied)


def _get_watchlist_cache_entry(cache_key: str) -> dict:
    with _watchlist_cache_lock:
        _prune_watchlist_cache_locked()
        in_memory = _copy_watchlist_entry(_watchlist_cache.get(cache_key))
        if in_memory:
            return in_memory
    disk_entry = _read_watchlist_cache_entry_from_disk(cache_key)
    if disk_entry:
        with _watchlist_cache_lock:
            _watchlist_cache[cache_key] = dict(disk_entry)
        return disk_entry
    return {}


def _extract_watchlist_error_ticker(message: str) -> str:
    text = str(message or "").strip()
    if not text:
        return ""
    head, _, _tail = text.partition(":")
    return _sanitize_ticker(head)


def _run_watchlist_scan_background(
    cache_key: str,
    tickers: list[str],
    universe_label: str,
    range_key: str,
    force_refresh: bool = False,
    seed_entry: dict | None = None,
    initial_delay_sec: float | None = None,
    scan_mode: str = "background",
) -> None:
    items_map: dict[str, list[str]] = {}
    no_match_set: set[str] = set()
    errors_by_ticker: dict[str, str] = {}

    if isinstance(seed_entry, dict):
        for item in list(seed_entry.get("items") or []):
            ticker = _sanitize_ticker(item.get("ticker"))
            alerts = [str(alert).strip() for alert in list(item.get("alerts") or []) if str(alert).strip()]
            if ticker and alerts:
                items_map[ticker] = alerts
        for ticker in list(seed_entry.get("no_match_tickers") or []):
            symbol = _sanitize_ticker(ticker)
            if symbol and symbol not in items_map:
                no_match_set.add(symbol)
        for err in list(seed_entry.get("errors") or []):
            symbol = _extract_watchlist_error_ticker(err)
            if symbol:
                errors_by_ticker[symbol] = str(err).strip()

    loaded_count = len(items_map) + len(no_match_set)

    for ticker in tickers:
        items_map.pop(ticker, None)
        no_match_set.discard(ticker)
        errors_by_ticker.pop(ticker, None)

    try:
        with _upstream_usage_scope("watchlist"):
            for idx, ticker in enumerate(tickers):
                response, status = _build_history_payload(ticker=ticker, range_key=range_key, force_refresh=force_refresh)
                if status != 200:
                    err = str(response.get("error") or "request failed").strip()
                    hint = str(response.get("hint") or "").strip()
                    errors_by_ticker[ticker] = f"{ticker}: {err}{' | ' + hint if hint else ''}"
                else:
                    loaded_count += 1
                    alerts = _detect_confluence_alerts_payload(response)
                    if alerts:
                        items_map[ticker] = [str(alert.get("text") or "") for alert in alerts if str(alert.get("text") or "").strip()]
                        no_match_set.discard(ticker)
                    else:
                        items_map.pop(ticker, None)
                        no_match_set.add(ticker)

                items = [{"ticker": symbol, "alerts": items_map[symbol]} for symbol in sorted(items_map)]
                no_match_tickers = sorted(no_match_set)
                errors = [errors_by_ticker[symbol] for symbol in sorted(errors_by_ticker)]
                _set_watchlist_cache_entry(
                    cache_key,
                    {
                        "ok": True,
                        "status": "running",
                        "day_key": _current_local_day_key(),
                        "universe_label": universe_label,
                        "range": range_key,
                        "tickers_considered": len(tickers),
                        "loaded_count": loaded_count,
                        "items": items,
                        "no_match_tickers": no_match_tickers,
                        "errors": errors,
                        "started_at": (_get_watchlist_cache_entry(cache_key).get("started_at") or datetime.now(timezone.utc).isoformat()),
                        "completed_at": "",
                        "last_ticker": ticker,
                        "cache_key": cache_key,
                        "force_refresh": force_refresh,
                        "delay_sec": float(initial_delay_sec or WATCHLIST_SCAN_DELAY_SEC),
                        "scan_mode": scan_mode,
                    },
                )

                provider_used = str(response.get("provider_used") or "")
                if idx < len(tickers) - 1 and (force_refresh or "cache" not in provider_used.lower()):
                    current_entry = _get_watchlist_cache_entry(cache_key)
                    current_delay = float(current_entry.get("delay_sec") or initial_delay_sec or WATCHLIST_SCAN_DELAY_SEC)
                    time.sleep(max(0.8, current_delay))

        items.sort(key=lambda item: str(item.get("ticker") or ""))
        no_match_tickers.sort()
        errors.sort()
        _set_watchlist_cache_entry(
            cache_key,
            {
                "ok": True,
                "status": "ready",
                "day_key": _current_local_day_key(),
                "universe_label": universe_label,
                "range": range_key,
                "tickers_considered": len(tickers),
                "loaded_count": loaded_count,
                "items": items,
                "no_match_tickers": no_match_tickers,
                "errors": errors,
                "started_at": (_get_watchlist_cache_entry(cache_key).get("started_at") or datetime.now(timezone.utc).isoformat()),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "last_ticker": tickers[-1] if tickers else "",
                "cache_key": cache_key,
                "force_refresh": force_refresh,
                "delay_sec": float(initial_delay_sec or WATCHLIST_SCAN_DELAY_SEC),
                "scan_mode": scan_mode,
            },
        )
    except Exception as exc:
        _set_watchlist_cache_entry(
            cache_key,
            {
                "ok": False,
                "status": "failed",
                "day_key": _current_local_day_key(),
                "universe_label": universe_label,
                "range": range_key,
                "tickers_considered": len(tickers),
                "loaded_count": loaded_count,
                "items": items,
                "no_match_tickers": no_match_tickers,
                "errors": errors + [f"watchlist scan failed: {exc}"],
                "started_at": (_get_watchlist_cache_entry(cache_key).get("started_at") or datetime.now(timezone.utc).isoformat()),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "last_ticker": "",
                "cache_key": cache_key,
                "force_refresh": force_refresh,
                "delay_sec": float(initial_delay_sec or WATCHLIST_SCAN_DELAY_SEC),
                "scan_mode": scan_mode,
            },
        )


def _ensure_watchlist_scan(
    tickers: list[str],
    universe_label: str,
    range_key: str,
    force_refresh: bool = False,
    retry_errors: bool = False,
    interactive: bool = False,
) -> dict:
    cache_key = _watchlist_cache_key(tickers, range_key)
    start_thread = False
    seed_entry: dict | None = None
    tickers_to_scan = list(tickers)
    requested_delay_sec = WATCHLIST_SCAN_DELAY_SEC if interactive else WATCHLIST_BACKGROUND_SCAN_DELAY_SEC
    requested_mode = "interactive" if interactive else "background"
    with _watchlist_cache_lock:
        _prune_watchlist_cache_locked()
        existing = _watchlist_cache.get(cache_key)
        if not existing:
            disk_entry = _read_watchlist_cache_entry_from_disk(cache_key)
            if disk_entry:
                _watchlist_cache[cache_key] = dict(disk_entry)
                existing = _watchlist_cache.get(cache_key)
        if existing and str(existing.get("status") or "") == "running":
            existing["delay_sec"] = min(float(existing.get("delay_sec") or requested_delay_sec), float(requested_delay_sec))
            if interactive:
                existing["scan_mode"] = "interactive"
            return _copy_watchlist_entry(existing)
        if existing and retry_errors and not force_refresh:
            failed_tickers = []
            for err in list(existing.get("errors") or []):
                symbol = _extract_watchlist_error_ticker(err)
                if symbol and symbol in tickers_to_scan and symbol not in failed_tickers:
                    failed_tickers.append(symbol)
            if failed_tickers:
                seed_entry = _copy_watchlist_entry(existing)
                tickers_to_scan = failed_tickers
            else:
                return _copy_watchlist_entry(existing)
        if existing and not force_refresh and not seed_entry:
            return _copy_watchlist_entry(existing)
        started_at = datetime.now(timezone.utc).isoformat()
        _watchlist_cache[cache_key] = {
            "ok": True,
            "status": "running",
            "day_key": _current_local_day_key(),
            "universe_label": universe_label,
            "range": range_key,
            "tickers_considered": len(tickers),
            "loaded_count": int((seed_entry or {}).get("loaded_count") or 0),
            "items": list((seed_entry or {}).get("items") or []),
            "no_match_tickers": list((seed_entry or {}).get("no_match_tickers") or []),
            "errors": list((seed_entry or {}).get("errors") or []),
            "started_at": started_at,
            "completed_at": "",
            "last_ticker": "",
            "cache_key": cache_key,
            "force_refresh": force_refresh,
            "delay_sec": float(requested_delay_sec),
            "scan_mode": requested_mode,
        }
        start_thread = True
        entry = _copy_watchlist_entry(_watchlist_cache[cache_key])

    if start_thread:
        _write_watchlist_cache_entry_to_disk(cache_key, entry)
        thread = threading.Thread(
            target=_run_watchlist_scan_background,
            args=(cache_key, tickers_to_scan, universe_label, range_key, force_refresh, seed_entry, requested_delay_sec, requested_mode),
            daemon=True,
            name=f"watchlist-prewarm-{range_key}",
        )
        thread.start()
    return entry


def _extract_close_series(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date", "close"])

    work = df.copy()
    if "date" not in work.columns:
        for candidate in ("Date", "Datetime", "index"):
            if candidate in work.columns:
                work = work.rename(columns={candidate: "date"})
                break
    if "date" not in work.columns:
        return pd.DataFrame(columns=["date", "close"])

    close_candidates = ["Close", "Adj Close"]
    close_candidates.extend([c for c in work.columns if str(c).endswith("_Close") or str(c).startswith("Close_")])
    close_candidates.extend(
        [c for c in work.columns if str(c).endswith("_Adj Close") or str(c).startswith("Adj Close_")]
    )
    close_col = next((c for c in close_candidates if c in work.columns), None)
    if not close_col:
        return pd.DataFrame(columns=["date", "close"])

    out = work[["date", close_col]].copy()
    out = out.rename(columns={close_col: "close"})
    out["date"] = pd.to_datetime(out["date"], utc=True, errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["date", "close"]).sort_values("date")
    return out


def _compute_rsi_points(close_df: pd.DataFrame, period: int = RSI_PERIOD) -> list[dict]:
    if close_df.empty or len(close_df.index) <= period:
        return []

    closes = close_df["close"].tolist()
    dates = close_df["date"].tolist()
    if len(closes) <= period:
        return []

    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains += max(diff, 0.0)
        losses += max(-diff, 0.0)
    avg_gain = gains / period
    avg_loss = losses / period

    def calc_rsi() -> float:
        if avg_loss == 0 and avg_gain == 0:
            return 50.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    out: list[dict] = [
        {"t": dates[period].isoformat(), "rsi": float(calc_rsi()), "close": float(closes[period])}
    ]
    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gain = max(diff, 0.0)
        loss = max(-diff, 0.0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period
        out.append({"t": dates[i].isoformat(), "rsi": float(calc_rsi()), "close": float(closes[i])})
    return out


def _analyze_intraday_rsi(ticker: str) -> dict:
    source = _get_market_data_source()
    df, _ = fetch_price_history_by_source(
        ticker=ticker,
        period="1d",
        interval="1m",
        prepost=True,
        source=source,
    )
    close_df = _extract_close_series(df)
    points = _compute_rsi_points(close_df, period=RSI_PERIOD)
    latest = points[-1] if points else None
    return {
        "ticker": ticker,
        "points_count": len(points),
        "latest": latest,
        "events": [],
    }


def _run_rsi_monitor_once(force_tickers: list[str] | None = None) -> dict:
    tickers = force_tickers if force_tickers is not None else _extract_watchlist_symbols_from_categories()
    tickers = [_sanitize_ticker(t) for t in tickers]
    tickers = [t for t in tickers if t]
    tickers = list(dict.fromkeys(tickers))

    latest: dict[str, dict] = {}
    errors: list[str] = []
    with _upstream_usage_scope("rsi_monitor"):
        for ticker in tickers:
            try:
                result = _analyze_intraday_rsi(ticker)
                latest[ticker] = {
                    "points_count": result["points_count"],
                    "latest": result["latest"],
                }
            except Exception as exc:
                errors.append(f"{ticker}: {exc}")

    with _rsi_monitor_lock:
        _rsi_monitor_state["last_run_utc"] = datetime.now(timezone.utc).isoformat()
        _rsi_monitor_state["watchlist"] = tickers
        _rsi_monitor_state["latest"] = latest
        _rsi_monitor_state["events"] = []
        if errors:
            _rsi_monitor_state["last_error"] = " | ".join(errors)[:4000]

    return {"watchlist": tickers, "latest": latest, "events": [], "errors": errors}


def _rsi_monitor_loop() -> None:
    with _rsi_monitor_lock:
        _rsi_monitor_state["running"] = True
    while not _rsi_monitor_stop.is_set():
        try:
            _run_rsi_monitor_once()
        except Exception as exc:
            with _rsi_monitor_lock:
                _rsi_monitor_state["last_error"] = str(exc)[:4000]
        if _rsi_monitor_stop.wait(max(60, RSI_MONITOR_INTERVAL_SEC)):
            break
    with _rsi_monitor_lock:
        _rsi_monitor_state["running"] = False


def _start_rsi_monitor_if_needed() -> None:
    global _rsi_monitor_thread
    with _rsi_monitor_lock:
        if _rsi_monitor_thread is not None and _rsi_monitor_thread.is_alive():
            return
        _rsi_monitor_stop.clear()
        _rsi_monitor_thread = threading.Thread(target=_rsi_monitor_loop, name="rsi-monitor", daemon=True)
        _rsi_monitor_thread.start()


def _sanitize_ticker(raw: str | None) -> str:
    return re.sub(r"[^A-Z0-9.\-]", "", (raw or "").upper()).strip()[:12]


def _sanitize_account(raw: str | None) -> str:
    return re.sub(r"\s+", " ", (raw or "").strip())[:80]


def _holding_key(symbol: str, account: str | None) -> str:
    return f"{_sanitize_ticker(symbol)}@@{_sanitize_account(account)}"


def _normalize_header(raw: str) -> str:
    return re.sub(r"\s+", " ", (raw or "").strip().lower())


def _parse_number(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    cleaned = text.replace(",", "").replace("$", "").replace("%", "")
    if cleaned.startswith("(") and cleaned.endswith(")"):
        cleaned = f"-{cleaned[1:-1]}"
    try:
        return float(cleaned)
    except ValueError:
        return None


def _pick_value(row: dict[str, str], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        if key in row:
            val = _parse_number(row.get(key))
            if val is not None:
                return val
    return None


def _extract_holdings_from_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))

    if not rows:
        return []

    normalized_rows = [[_normalize_header(cell) for cell in row] for row in rows]
    header_idx = 0
    for idx, row in enumerate(normalized_rows):
        has_symbol = any(("symbol" in c or "ticker" in c) for c in row)
        has_qty = any(any(token in c for token in ("quantity", "shares", "qty")) for c in row)
        if has_symbol and has_qty:
            header_idx = idx
            break

    headers = rows[header_idx] if header_idx < len(rows) else []
    if not headers:
        return []

    key_map = {_normalize_header(name): i for i, name in enumerate(headers)}

    symbol_keys = tuple(k for k in key_map.keys() if ("symbol" in k or "ticker" in k))
    account_name_keys = tuple(k for k in key_map.keys() if ("account name" in k or k == "account"))
    account_num_keys = tuple(k for k in key_map.keys() if ("account number" in k or "acct" in k))
    qty_keys = tuple(
        k
        for k in key_map.keys()
        if any(token in k for token in ("quantity", "shares", "qty"))
    )
    avg_cost_keys = tuple(
        k
        for k in key_map.keys()
        if any(
            token in k
            for token in (
                "average cost",
                "average cost basis",
                "avg cost",
                "cost/share",
                "cost per share",
                "purchase price",
                "price paid",
                "avg price",
            )
        )
    )
    total_cost_keys = tuple(
        k
        for k in key_map.keys()
        if any(token in k for token in ("cost basis", "total cost", "cost basis total"))
    )

    data_rows = rows[header_idx + 1 :]
    out: list[dict] = []

    for raw_row in data_rows:
        if not raw_row:
            continue

        row: dict[str, str] = {}
        for norm_key, col_idx in key_map.items():
            row[norm_key] = raw_row[col_idx] if col_idx < len(raw_row) else ""

        symbol = ""
        for key in symbol_keys:
            symbol = _sanitize_ticker(row.get(key))
            if symbol:
                break

        if not symbol and raw_row:
            symbol = _sanitize_ticker(raw_row[0])
        if not symbol:
            continue

        account_name = ""
        for key in account_name_keys:
            account_name = _sanitize_account(row.get(key))
            if account_name:
                break

        account_num = ""
        for key in account_num_keys:
            account_num = _sanitize_account(row.get(key))
            if account_num:
                break

        account = account_name or account_num or None

        shares = _pick_value(row, qty_keys) if qty_keys else None
        avg_cost = _pick_value(row, avg_cost_keys) if avg_cost_keys else None
        if avg_cost is None and shares and shares != 0 and total_cost_keys:
            total_cost = _pick_value(row, total_cost_keys)
            if total_cost is not None:
                avg_cost = total_cost / shares
        # Skip non-position rows (totals, notes, cash-summary lines) that do not carry share quantity.
        if shares is None:
            continue

        out.append(
            {
                "symbol": symbol,
                "account": account,
                "shares": float(shares),
                "avg_cost": float(avg_cost) if avg_cost is not None else None,
            }
        )

    merged: dict[str, dict] = {}
    for h in out:
        sym = h["symbol"]
        acct = _sanitize_account(h.get("account")) if h.get("account") else ""
        key = _holding_key(sym, acct)
        shares = h.get("shares")
        avg_cost = h.get("avg_cost")

        if key not in merged:
            merged[key] = {"symbol": sym, "account": acct or None, "shares": shares, "avg_cost": avg_cost}
            continue

        cur = merged[key]
        cur_shares = cur.get("shares")
        cur_avg = cur.get("avg_cost")

        if shares is not None and cur_shares is not None and shares >= 0 and cur_shares >= 0:
            total_shares = cur_shares + shares
            if total_shares > 0:
                if cur_avg is not None and avg_cost is not None:
                    weighted = ((cur_shares * cur_avg) + (shares * avg_cost)) / total_shares
                else:
                    weighted = cur_avg if cur_avg is not None else avg_cost
                cur["shares"] = total_shares
                cur["avg_cost"] = weighted
        else:
            if cur_shares is None:
                cur["shares"] = shares
            if cur_avg is None:
                cur["avg_cost"] = avg_cost

    return list(merged.values())


def _load_portfolio_config() -> list[dict]:
    _ensure_portfolio_storage()
    if not PORTFOLIO_CONFIG_PATH.exists():
        return []
    with PORTFOLIO_CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    return [d for d in data if isinstance(d, dict)]


def _save_portfolio_config(items: list[dict]) -> None:
    _ensure_portfolio_storage()
    with PORTFOLIO_CONFIG_PATH.open("w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)


def _materialize_portfolio(item: dict) -> dict:
    portfolio_id = str(item.get("id") or uuid.uuid4().hex[:10])
    name = (str(item.get("name") or "Imported Holdings").strip() or "Imported Holdings")[:80]
    csv_rel = str(item.get("csv_path") or "")
    csv_path = (BASE_DIR / csv_rel).resolve() if csv_rel else None

    selected_keys = {str(s).strip() for s in item.get("selected_keys", []) if str(s).strip()}
    removed_keys = {str(s).strip() for s in item.get("removed_keys", []) if str(s).strip()}
    symbol_overrides = {
        str(k).strip(): _sanitize_ticker(v)
        for k, v in (item.get("symbol_overrides") or {}).items()
        if str(k).strip() and _sanitize_ticker(v)
    }

    # backward compatibility with older symbol-only config
    selected_symbols = {_sanitize_ticker(s) for s in item.get("selected_symbols", [])}
    removed_symbols = {_sanitize_ticker(s) for s in item.get("removed_symbols", [])}
    selected_symbols.discard("")
    removed_symbols.discard("")

    tickers: list[dict] = []
    missing_file = False
    if csv_path and csv_path.exists():
        try:
            holdings = _extract_holdings_from_csv(csv_path)
            for h in holdings:
                original_symbol = _sanitize_ticker(h.get("symbol"))
                s = original_symbol
                acct = _sanitize_account(h.get("account")) if h.get("account") else ""
                key = _holding_key(s, acct)
                override_symbol = symbol_overrides.get(key)
                if override_symbol:
                    s = override_symbol
                if not s:
                    continue
                if key in removed_keys or s in removed_symbols:
                    continue

                selected = key in selected_keys
                if not selected and s in selected_symbols:
                    selected = True

                tickers.append(
                    {
                        "id": key,
                        "symbol": s,
                        "original_symbol": original_symbol,
                        "account": acct or None,
                        "selected": selected,
                        "shares": h.get("shares"),
                        "avg_cost": h.get("avg_cost"),
                    }
                )
        except Exception:
            tickers = []
    else:
        missing_file = True

    valid_symbols = {t["symbol"] for t in tickers}
    valid_keys = {t["id"] for t in tickers if t.get("id")}
    return {
        "id": portfolio_id,
        "name": name,
        "collapsed": bool(item.get("collapsed", False)),
        "csv_path": csv_rel,
        "tickers": tickers,
        "selected_keys": sorted([k for k in selected_keys if k in valid_keys]),
        "removed_keys": sorted(list(removed_keys)),
        "selected_symbols": sorted([s for s in selected_symbols if s in valid_symbols]),
        "removed_symbols": sorted(list(removed_symbols)),
        "symbol_overrides": symbol_overrides,
        "missing_file": missing_file,
    }




def _load_category_config() -> list[dict]:
    _ensure_category_storage()
    if not CATEGORY_CONFIG_PATH.exists():
        return []
    with CATEGORY_CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    return [d for d in data if isinstance(d, dict)]


def _save_category_config(items: list[dict]) -> None:
    _ensure_category_storage()
    with CATEGORY_CONFIG_PATH.open("w", encoding="utf-8") as f:
        json.dump(items, f, indent=2)


def _to_chart_points(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []

    if "date" not in df.columns:
        for candidate in ("Date", "Datetime", "index"):
            if candidate in df.columns:
                df = df.rename(columns={candidate: "date"})
                break

    if "date" not in df.columns:
        raise ValueError(f"Missing timestamp column. Columns={list(df.columns)}")

    open_candidates = ["Open"]
    high_candidates = ["High"]
    low_candidates = ["Low"]
    close_candidates = ["Close", "Adj Close"]
    volume_candidates = ["Volume"]

    open_candidates.extend([c for c in df.columns if str(c).endswith("_Open") or str(c).startswith("Open_")])
    high_candidates.extend([c for c in df.columns if str(c).endswith("_High") or str(c).startswith("High_")])
    low_candidates.extend([c for c in df.columns if str(c).endswith("_Low") or str(c).startswith("Low_")])
    close_candidates.extend([c for c in df.columns if str(c).endswith("_Close") or str(c).startswith("Close_")])
    close_candidates.extend(
        [c for c in df.columns if str(c).endswith("_Adj Close") or str(c).startswith("Adj Close_")]
    )
    volume_candidates.extend([c for c in df.columns if str(c).endswith("_Volume") or str(c).startswith("Volume_")])

    def pick_value(row: pd.Series, candidates: list[str]) -> float | None:
        for c in candidates:
            val = row.get(c)
            if not pd.isna(val):
                return float(val)
        return None

    points = []
    for _, row in df.iterrows():
        dt = pd.to_datetime(row["date"], utc=True)
        open_val = pick_value(row, open_candidates)
        high_val = pick_value(row, high_candidates)
        low_val = pick_value(row, low_candidates)
        close_val = pick_value(row, close_candidates)
        volume_val = pick_value(row, volume_candidates) or 0.0

        if close_val is None:
            continue

        points.append(
            {
                "t": dt.isoformat(),
                "open": open_val,
                "high": high_val,
                "low": low_val,
                "close": close_val,
                "volume": volume_val,
            }
        )
    return points


def _trim_df_to_recent_trading_sessions(df: pd.DataFrame, session_count: int) -> pd.DataFrame:
    if df.empty or session_count <= 0:
        return df
    working = df.copy()
    if "date" not in working.columns:
        for candidate in ("Date", "Datetime", "index"):
            if candidate in working.columns:
                working = working.rename(columns={candidate: "date"})
                break
    if "date" not in working.columns:
        return working
    parsed = pd.to_datetime(working["date"], utc=True, errors="coerce")
    if parsed.isna().all():
        return working
    session_labels = parsed.dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d")
    unique_sessions = [str(label) for label in pd.unique(session_labels.dropna()) if str(label)]
    if len(unique_sessions) <= session_count:
        return working
    keep_sessions = set(unique_sessions[-session_count:])
    mask = session_labels.isin(keep_sessions)
    trimmed = working.loc[mask].copy()
    return trimmed.reset_index(drop=True)


def _filter_regular_session_points(points: list[dict]) -> list[dict]:
    out_with_day: list[tuple[dict, str]] = []
    for pt in points:
        ts = pt.get("t")
        dt = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(dt):
            continue
        dt_et = dt.tz_convert("America/New_York")
        if dt_et.weekday() >= 5:
            continue
        mins = dt_et.hour * 60 + dt_et.minute
        if (9 * 60 + 30) <= mins <= (16 * 60):
            out_with_day.append((pt, dt_et.strftime("%Y-%m-%d")))

    if not out_with_day:
        return []

    latest_day = max(day for _, day in out_with_day)
    return [pt for pt, day in out_with_day if day == latest_day]


def _to_moving_average_points(df: pd.DataFrame, windows: dict[str, int]) -> dict[str, list[dict]]:
    if df.empty or not windows:
        return {}

    if "date" not in df.columns:
        for candidate in ("Date", "Datetime", "index"):
            if candidate in df.columns:
                df = df.rename(columns={candidate: "date"})
                break
    if "date" not in df.columns:
        return {}

    close_candidates = ["Close", "Adj Close"]
    close_candidates.extend([c for c in df.columns if str(c).endswith("_Close") or str(c).startswith("Close_")])
    close_candidates.extend(
        [c for c in df.columns if str(c).endswith("_Adj Close") or str(c).startswith("Adj Close_")]
    )
    close_col = next((c for c in close_candidates if c in df.columns), None)
    if not close_col:
        return {}

    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], utc=True, errors="coerce")
    work["close"] = pd.to_numeric(work[close_col], errors="coerce")
    work = work.dropna(subset=["date", "close"]).sort_values("date")
    if work.empty:
        return {}

    out: dict[str, list[dict]] = {}
    for key, window in windows.items():
        if window <= 0:
            continue
        series = work["close"].rolling(window=window, min_periods=window).mean()
        pts: list[dict] = []
        for dt, val in zip(work["date"], series):
            if pd.isna(val):
                continue
            pts.append({"t": dt.isoformat(), "v": float(val)})
        out[key] = pts
    return out


def _compute_bollinger_bands_pd(close: pd.Series, window: int = 20, num_std: float = 2.0) -> tuple[pd.Series, pd.Series]:
    mean = close.rolling(window=window, min_periods=window).mean()
    std = close.rolling(window=window, min_periods=window).std(ddof=0)
    upper = mean + (std * num_std)
    lower = mean - (std * num_std)
    return upper, lower


def _build_history_payload(
    ticker: str,
    range_key: str,
    force_refresh: bool = False,
    chart_type: str = "line",
) -> tuple[dict, int]:
    ticker = _sanitize_ticker(ticker)
    range_key = str(range_key or "1m").strip().lower()
    chart_type = str(chart_type or "line").strip().lower()
    if not ticker:
        return {"error": "Ticker is required."}, 400
    if range_key not in RANGE_MAP:
        return {"error": f"Unsupported range '{range_key}'."}, 400

    period, interval = RANGE_MAP[range_key]
    if chart_type == "candles" and range_key == "1d":
        interval = "10m"
    elif chart_type == "candles" and range_key != "1d":
        interval = "1d"
    source = _get_market_data_source()
    use_prepost = range_key == "1d"
    ttl_history_sec = 0 if force_refresh else 300

    try:
        df, provider_used = _fetch_price_history_cached(
            ticker=ticker,
            period=period,
            interval=interval,
            prepost=use_prepost,
            source=source,
            ttl_sec=ttl_history_sec,
        )
        if df.empty and range_key == "1d":
            df, provider_used = _fetch_price_history_cached(
                ticker=ticker,
                period="1d",
                interval="5m",
                prepost=True,
                source=source,
                ttl_sec=ttl_history_sec,
            )

        target_sessions = TRADING_DAY_TARGETS.get(range_key)
        if target_sessions and range_key != "1d":
            if interval == "1d":
                df = df.tail(target_sessions).copy()
            else:
                df = _trim_df_to_recent_trading_sessions(df, target_sessions)

        points = _to_chart_points(df)
        if range_key == "1d":
            points = _filter_regular_session_points(points)
        if not points:
            extra_hint = ""
            if source == "twelve_data" and not _get_config_value("TWELVE_DATA_API_KEY"):
                extra_hint = " TWELVE_DATA_API_KEY is not set in this server process."
            diagnosis = diagnose_source_failure(ticker=ticker, period=period, interval=interval, source=source)
            if diagnosis:
                extra_hint = f"{extra_hint} {diagnosis}".strip()
            return (
                {
                    "error": "No market data returned from upstream provider.",
                    "hint": (
                        "Upstream may be rate-limiting (HTTP 429) or TLS/certificate connectivity may be failing."
                        + extra_hint
                    ),
                    "ticker": ticker,
                    "range": range_key,
                    "period": period,
                    "interval": interval,
                    "source": source,
                    "provider_used": provider_used,
                    "count": 0,
                    "points": [],
                    "as_of_utc": datetime.now(timezone.utc).isoformat(),
                },
                502,
            )

        ma_points: dict[str, list[dict]] = {}
        try:
            daily_df, _ = _fetch_price_history_cached(
                ticker=ticker,
                period="2y",
                interval="1d",
                prepost=False,
                source=source,
                ttl_sec=3600,
            )
            ma_points = _to_moving_average_points(daily_df, {"ma50": 50, "ma200": 200})
        except Exception:
            ma_points = {}

        return (
            {
                "ticker": ticker,
                "range": range_key,
                "period": period,
                "interval": interval,
                "source": source,
                "provider_used": provider_used,
                "count": len(points),
                "points": points,
                "moving_averages": ma_points,
                "as_of_utc": datetime.now(timezone.utc).isoformat(),
            },
            200,
        )
    except Exception as exc:  # pragma: no cover
        return (
            {
                "error": str(exc),
                "type": exc.__class__.__name__,
                "ticker": ticker,
                "range": range_key,
            },
            500,
        )


def _detect_confluence_alerts_payload(payload: dict) -> list[dict]:
    points = payload.get("points") or []
    if not isinstance(points, list) or len(points) < 40:
        return []

    work = pd.DataFrame(points)
    if work.empty:
        return []
    work["close"] = pd.to_numeric(work.get("close"), errors="coerce")
    work["volume"] = pd.to_numeric(work.get("volume"), errors="coerce").fillna(0.0)
    work["t"] = work.get("t", "").astype(str)
    work = work.dropna(subset=["close"])
    if len(work) < 30:
        return []

    close = work["close"].astype(float)
    rsi = _compute_rsi_series_pd(close, 14)
    macd, signal, hist = _compute_macd_pd(close)
    upper, lower = _compute_bollinger_bands_pd(close, 20, 2.0)

    if len(hist.dropna()) < 3:
        return []

    latest = work.iloc[-1]
    latest_close = float(latest["close"])
    latest_vol = float(latest["volume"] or 0.0)
    rsi_now = float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else None
    upper_now = float(upper.iloc[-1]) if pd.notna(upper.iloc[-1]) else None
    lower_now = float(lower.iloc[-1]) if pd.notna(lower.iloc[-1]) else None
    hist_now = float(hist.iloc[-1]) if pd.notna(hist.iloc[-1]) else None
    hist_prev = float(hist.iloc[-2]) if pd.notna(hist.iloc[-2]) else None
    hist_prev2 = float(hist.iloc[-3]) if pd.notna(hist.iloc[-3]) else None
    macd_now = float(macd.iloc[-1]) if pd.notna(macd.iloc[-1]) else None
    signal_now = float(signal.iloc[-1]) if pd.notna(signal.iloc[-1]) else None

    if None in {rsi_now, upper_now, lower_now, hist_now, hist_prev, hist_prev2, macd_now, signal_now}:
        return []

    recent_vol = work["volume"].tail(20)
    recent_vol = recent_vol[recent_vol > 0]
    avg_vol20 = float(recent_vol.mean()) if not recent_vol.empty else 0.0

    ma_daily = payload.get("moving_averages") or {}
    ma50_pts = ma_daily.get("ma50") or []
    ma200_pts = ma_daily.get("ma200") or []
    ma50_raw = float(ma50_pts[-1]["v"]) if ma50_pts else None
    ma200_raw = float(ma200_pts[-1]["v"]) if ma200_pts else None

    near_upper = latest_close >= (upper_now * 0.995)
    near_lower = latest_close <= (lower_now * 1.005)
    hist_falling = hist_now < hist_prev < hist_prev2
    hist_rising = hist_now > hist_prev > hist_prev2
    volume_fading = avg_vol20 > 0 and latest_vol < (avg_vol20 * 0.9)
    volume_rising = avg_vol20 > 0 and latest_vol > (avg_vol20 * 1.2)

    alerts: list[dict] = []
    ticker = str(payload.get("ticker") or "").upper()

    if rsi_now >= 65 and near_upper and hist_falling and volume_fading:
        alerts.append(
            {
                "ticker": ticker,
                "kind": "reversal_watch",
                "text": f"{ticker}: Reversal watch (RSI {rsi_now:.1f}, near upper band, MACD momentum fading, volume cooling).",
            }
        )

    trend_up = (ma50_raw is not None and latest_close > ma50_raw) or (ma200_raw is not None and latest_close > ma200_raw)
    if 52 <= rsi_now <= 68 and macd_now > signal_now and hist_rising and trend_up and volume_rising:
        alerts.append(
            {
                "ticker": ticker,
                "kind": "trend_up_watch",
                "text": f"{ticker}: Uptrend continuation watch (RSI rising, MACD bullish, above MA trend filter, volume confirming).",
            }
        )

    band_width_pct = ((upper_now - lower_now) / max(abs(latest_close), 1e-9)) * 100.0
    if band_width_pct < 1.2 and rsi_now > 50 and macd_now > signal_now and volume_rising:
        alerts.append(
            {
                "ticker": ticker,
                "kind": "squeeze_breakout_watch",
                "text": f"{ticker}: Bollinger squeeze breakout watch (tight bands + RSI>50 + MACD bullish + volume pickup).",
            }
        )

    if rsi_now <= 35 and near_lower and hist_now > hist_prev and volume_rising:
        alerts.append(
            {
                "ticker": ticker,
                "kind": "bounce_watch",
                "text": f"{ticker}: Oversold bounce watch (RSI {rsi_now:.1f}, near lower band, MACD histogram improving).",
            }
        )

    return alerts


def _acquire_twelve_rate_limit_slot() -> None:
    while True:
        sleep_for = 0.0
        now = time.time()
        with _twelve_rate_limit_lock:
            while _twelve_rate_limit_hits and (now - _twelve_rate_limit_hits[0]) >= TWELVE_RATE_LIMIT_WINDOW_SEC:
                _twelve_rate_limit_hits.popleft()
            if len(_twelve_rate_limit_hits) < TWELVE_RATE_LIMIT_PER_MIN:
                _twelve_rate_limit_hits.append(now)
                return
            oldest = _twelve_rate_limit_hits[0]
            sleep_for = max(0.25, TWELVE_RATE_LIMIT_WINDOW_SEC - (now - oldest) + 0.05)
        time.sleep(sleep_for)


def _fetch_price_history_cached(
    ticker: str,
    period: str,
    interval: str,
    prepost: bool = False,
    source: str = "auto",
    ttl_sec: int = 60,
    stale_ttl_sec: int = 6 * 3600,
) -> tuple[pd.DataFrame, str]:
    key = (ticker, period, interval, bool(prepost), (source or "auto").strip().lower())
    now = time.time()

    # --- L1: in-memory cache ---
    hit = None
    with _history_cache_lock:
        hit = _history_cache.get(key)
        if hit is not None and (now - float(hit.get("at", 0.0))) < max(1, ttl_sec):
            cached_df = hit.get("df")
            if isinstance(cached_df, pd.DataFrame):
                return cached_df.copy(), str(hit.get("provider") or "cache")

    # --- L2: disk cache ---
    disk_ttl = max(ttl_sec, _market_aware_ttl(interval))
    l2_df, l2_fetched_at = _disk_cache.get(ticker, period, interval, prepost, max_age_sec=disk_ttl)
    if l2_df is not None and not l2_df.empty:
        # Promote to L1
        with _history_cache_lock:
            _history_cache[key] = {"at": l2_fetched_at, "df": l2_df.copy(), "provider": "disk_cache"}
        return l2_df.copy(), "disk_cache"

    # --- L3: upstream API ---
    if (source or "auto").strip().lower() == "twelve_data":
        _acquire_twelve_rate_limit_slot()
    df, provider = fetch_price_history_by_source(
        ticker=ticker,
        period=period,
        interval=interval,
        prepost=prepost,
        source=source,
    )
    if not df.empty:
        with _history_cache_lock:
            _history_cache[key] = {"at": now, "df": df.copy(), "provider": provider}
        _disk_cache.put(ticker, period, interval, prepost, df)
        return df, provider

    # --- Fallback: stale L1 ---
    if hit is not None:
        cached_df = hit.get("df")
        cached_at = float(hit.get("at", 0.0))
        if isinstance(cached_df, pd.DataFrame) and not cached_df.empty and (now - cached_at) < max(60, stale_ttl_sec):
            return cached_df.copy(), str(hit.get("provider") or "cache_stale")

    # --- Fallback: stale L2 (up to 24h) ---
    stale_df = _disk_cache.get_stale(ticker, period, interval, prepost, max_stale_sec=86400)
    if stale_df is not None and not stale_df.empty:
        return stale_df.copy(), "disk_cache_stale"

    return df, provider


_setup_logging()


@app.before_request
def _log_request_start():
    g.request_started_at = time.perf_counter()


@app.after_request
def _log_request_end(response):
    started = getattr(g, "request_started_at", None)
    elapsed_ms = None
    if isinstance(started, (int, float)):
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)

    app.logger.info(
        "request method=%s path=%s status=%s elapsed_ms=%s ip=%s query=%s",
        request.method,
        request.path,
        response.status_code,
        elapsed_ms,
        request.remote_addr,
        dict(request.args),
    )
    return response


@app.post("/api/client-log")
def api_client_log():
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action") or "").strip()
    if not action:
        return jsonify({"error": "action is required"}), 400

    details = payload.get("details")
    if details is None:
        details = {}
    if not isinstance(details, dict):
        details = {"value": str(details)}

    entry = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "session_id": str(payload.get("session_id") or ""),
        "action": action[:120],
        "details": details,
        "path": request.path,
        "ip": request.remote_addr,
    }

    try:
        _append_client_action_log(entry)
    except Exception as exc:
        app.logger.exception("failed_to_write_client_action_log: %s", exc)
        return jsonify({"error": "failed to write log"}), 500

    return jsonify({"ok": True})


@app.get("/api/log-paths")
def api_log_paths():
    _ensure_log_storage()
    return jsonify(
        {
            "server_log": str(SERVER_LOG_PATH),
            "client_action_log": str(CLIENT_ACTION_LOG_PATH),
            "upstream_api_log": str(UPSTREAM_API_LOG_PATH),
        }
    )


@app.get("/api/upstream-usage")
def api_upstream_usage():
    limit = request.args.get("limit", "500")
    try:
        max_entries = max(50, min(2000, int(limit)))
    except Exception:
        max_entries = 500
    snapshot = _build_upstream_usage_snapshot(max_entries=max_entries)
    snapshot["log_path"] = str(UPSTREAM_API_LOG_PATH)
    snapshot["max_entries"] = max_entries
    return jsonify(snapshot)


@app.get("/")
def index():
    return render_template("index.html")




@app.get("/api/categories")
def api_get_categories():
    categories = _load_category_config()
    return jsonify({"categories": categories})


@app.post("/api/categories/save")
def api_save_categories():
    payload = request.get_json(silent=True) or {}
    categories = payload.get("categories")
    if not isinstance(categories, list):
        return jsonify({"error": "Invalid payload: categories[] required."}), 400

    cleaned: list[dict] = []
    for c in categories:
        if not isinstance(c, dict):
            continue
        cid = str(c.get("id") or "").strip()
        if not cid:
            continue
        name = (str(c.get("name") or "Category").strip() or "Category")[:80]
        items = c.get("items") if isinstance(c.get("items"), list) else []
        cleaned_items = []
        for it in items:
            if not isinstance(it, dict):
                continue
            symbol = _sanitize_ticker(it.get("symbol"))
            if not symbol:
                continue
            cleaned_items.append(
                {
                    "id": str(it.get("id") or _holding_key(symbol, it.get("account"))).strip(),
                    "symbol": symbol,
                    "account": _sanitize_account(it.get("account")) or None,
                    "label": str(it.get("label") or symbol)[:120],
                    "selected": bool(it.get("selected", False)),
                    "source": str(it.get("source") or "manual")[:24],
                    "source_tab": _sanitize_account(it.get("source_tab") or it.get("sourceTab")) or None,
                    "source_portfolio_id": str(it.get("source_portfolio_id") or it.get("sourcePortfolioId") or "").strip()[:80] or None,
                    "source_holding_id": str(it.get("source_holding_id") or it.get("sourceHoldingId") or "").strip()[:120] or None,
                    "shares": _parse_number(it.get("shares")),
                }
            )
        cleaned.append({"id": cid, "name": name, "items": cleaned_items})

    _save_category_config(cleaned)
    return jsonify({"saved": True, "categories": cleaned})


@app.get("/api/portfolios")
def api_get_portfolios():
    items = _load_portfolio_config()
    portfolios = [_materialize_portfolio(item) for item in items]
    if portfolios and all(p["collapsed"] for p in portfolios):
        portfolios[0]["collapsed"] = False
    return jsonify({"portfolios": portfolios})


@app.post("/api/portfolios/import")
def api_import_portfolio():
    name = (request.form.get("name") or "Imported Holdings").strip() or "Imported Holdings"
    upload = request.files.get("file")
    if upload is None or not upload.filename:
        return jsonify({"error": "CSV file is required."}), 400

    _ensure_portfolio_storage()

    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", Path(upload.filename).name)
    if not safe_name.lower().endswith(".csv"):
        safe_name = f"{safe_name}.csv"

    portfolio_id = uuid.uuid4().hex[:10]
    stored_name = f"{portfolio_id}_{safe_name}"
    stored_path = PORTFOLIO_FILES_DIR / stored_name
    upload.save(stored_path)

    try:
        holdings = _extract_holdings_from_csv(stored_path)
    except Exception as exc:
        try:
            stored_path.unlink(missing_ok=True)
        except Exception:
            pass
        return jsonify({"error": f"Unable to parse CSV: {exc}"}), 400

    if not holdings:
        try:
            stored_path.unlink(missing_ok=True)
        except Exception:
            pass
        return jsonify({"error": "No symbols found in CSV."}), 400

    items = _load_portfolio_config()

    # Replace existing tab with the same display name (case-insensitive) to avoid duplicates on re-import.
    # Preserve the prior portfolio identity and selection/removal state so periodic CSV refreshes
    # keep the user's category links and imported-holdings workflow intact.
    normalized_name = name[:80].strip().lower()
    kept_items = []
    existing_item = None
    for item in items:
        item_name = str(item.get("name") or "").strip().lower()
        if item_name == normalized_name:
            existing_item = item
            old_csv_rel = str(item.get("csv_path") or "").strip()
            if old_csv_rel:
                try:
                    (BASE_DIR / old_csv_rel).unlink(missing_ok=True)
                except Exception:
                    pass
            continue
        kept_items.append(item)

    items = kept_items
    existing_id = str((existing_item or {}).get("id") or "").strip()
    items.insert(
        0,
        {
            "id": existing_id or portfolio_id,
            "name": name[:80],
            "csv_path": str(stored_path.relative_to(BASE_DIR)),
            "collapsed": bool((existing_item or {}).get("collapsed", False)),
            "selected_keys": list((existing_item or {}).get("selected_keys", [])),
            "removed_keys": list((existing_item or {}).get("removed_keys", [])),
            "selected_symbols": list((existing_item or {}).get("selected_symbols", [])),
            "removed_symbols": list((existing_item or {}).get("removed_symbols", [])),
            "symbol_overrides": dict((existing_item or {}).get("symbol_overrides") or {}),
            "created_at": str((existing_item or {}).get("created_at") or datetime.now(timezone.utc).isoformat()),
        },
    )

    for i in range(1, len(items)):
        items[i]["collapsed"] = True

    _save_portfolio_config(items)
    portfolios = [_materialize_portfolio(item) for item in items]
    return jsonify({"portfolios": portfolios})


@app.post("/api/portfolios/save")
def api_save_portfolios():
    payload = request.get_json(silent=True) or {}
    portfolios = payload.get("portfolios")
    if not isinstance(portfolios, list):
        return jsonify({"error": "Invalid payload: portfolios[] required."}), 400

    current = {str(p.get("id")): p for p in _load_portfolio_config()}
    next_items: list[dict] = []

    for incoming in portfolios:
        if not isinstance(incoming, dict):
            continue

        pid = str(incoming.get("id") or "").strip()
        if not pid or pid not in current:
            continue

        base = current[pid]
        selected_keys = [str(s).strip() for s in incoming.get("selected_keys", []) if str(s).strip()]
        removed_keys = [str(s).strip() for s in incoming.get("removed_keys", []) if str(s).strip()]
        selected = [_sanitize_ticker(s) for s in incoming.get("selected_symbols", []) if _sanitize_ticker(s)]
        removed = [_sanitize_ticker(s) for s in incoming.get("removed_symbols", []) if _sanitize_ticker(s)]
        symbol_overrides = {
            str(k).strip(): _sanitize_ticker(v)
            for k, v in (incoming.get("symbol_overrides") or {}).items()
            if str(k).strip() and _sanitize_ticker(v)
        }

        next_items.append(
            {
                "id": pid,
                "name": (str(incoming.get("name") or base.get("name") or "Imported Holdings").strip() or "Imported Holdings")[:80],
                "csv_path": str(base.get("csv_path") or ""),
                "collapsed": bool(incoming.get("collapsed", base.get("collapsed", False))),
                "selected_keys": sorted(list(set(selected_keys))),
                "removed_keys": sorted(list(set(removed_keys))),
                "selected_symbols": sorted(list(set(selected))),
                "removed_symbols": sorted(list(set(removed))),
                "symbol_overrides": symbol_overrides,
                "created_at": base.get("created_at"),
            }
        )

    _save_portfolio_config(next_items)
    materialized = [_materialize_portfolio(item) for item in next_items]
    if materialized and all(p["collapsed"] for p in materialized):
        materialized[0]["collapsed"] = False
    return jsonify({"saved": True, "portfolios": materialized})


@app.get("/api/history")
def api_history():
    ticker = (request.args.get("ticker") or "NVDA").strip().upper()
    range_key = (request.args.get("range") or "1m").strip().lower()
    force_refresh = (request.args.get("refresh") or "").strip().lower() in {"1", "true", "yes", "on"}
    chart_type = (request.args.get("chart_type") or "line").strip().lower()
    with _upstream_usage_scope("chart"):
        response, status = _build_history_payload(
            ticker=ticker,
            range_key=range_key,
            force_refresh=force_refresh,
            chart_type=chart_type,
        )
    return jsonify(response), status


@app.get("/api/rsi-monitor/status")
def api_rsi_monitor_status():
    ticker = _sanitize_ticker(request.args.get("ticker"))
    with _rsi_monitor_lock:
        state = {
            "running": bool(_rsi_monitor_state.get("running")),
            "interval_sec": int(_rsi_monitor_state.get("interval_sec") or RSI_MONITOR_INTERVAL_SEC),
            "last_run_utc": _rsi_monitor_state.get("last_run_utc"),
            "watchlist": list(_rsi_monitor_state.get("watchlist") or []),
            "latest": dict(_rsi_monitor_state.get("latest") or {}),
            "events": list(_rsi_monitor_state.get("events") or []),
            "last_error": _rsi_monitor_state.get("last_error") or "",
        }
    if ticker:
        state["latest"] = {ticker: state["latest"].get(ticker)}
        state["events"] = [e for e in state["events"] if str(e.get("ticker")) == ticker]
    return jsonify(state)


def _call_openai(api_key: str, prompt: str, trend_img: str, regime_img: str, override_model: str = "") -> tuple[str, str, dict | None]:
    model = override_model.strip() if override_model else (_get_config_value("CHART_AI_MODEL") or "gpt-4.1-mini")
    body = {
        "model": model,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_text", "text": "Trend view screenshot:"},
                    {"type": "input_image", "image_url": trend_img},
                    {"type": "input_text", "text": "Regime view screenshot:"},
                    {"type": "input_image", "image_url": regime_img},
                ],
            }
        ],
    }
    req_data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=req_data,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        return "", model, {"error": "OpenAI request failed", "detail": detail[:2000], "status": 502}
    except Exception as exc:
        return "", model, {"error": "OpenAI request failed", "detail": str(exc)[:1000], "status": 502}
    try:
        obj = json.loads(raw)
    except Exception:
        return "", model, {"error": "OpenAI response parse failed", "detail": raw[:2000], "status": 502}
    text = _extract_response_text(obj)
    if not text:
        return "", model, {"error": "No explanation text returned from model.", "status": 502}
    return text, model, None


def _call_openai_text(api_key: str, prompt: str, override_model: str = "") -> tuple[str, str, dict | None]:
    model = override_model.strip() if override_model else (_get_config_value("CHART_AI_MODEL") or "gpt-4.1-mini")
    body = {
        "model": model,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                ],
            }
        ],
    }
    req_data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=req_data,
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        return "", model, {"error": "OpenAI request failed", "detail": detail[:2000], "status": 502}
    except Exception as exc:
        return "", model, {"error": "OpenAI request failed", "detail": str(exc)[:1000], "status": 502}
    try:
        obj = json.loads(raw)
    except Exception:
        return "", model, {"error": "OpenAI response parse failed", "detail": raw[:2000], "status": 502}
    text = _extract_response_text(obj)
    if not text:
        return "", model, {"error": "No explanation text returned from model.", "status": 502}
    return text, model, None


def _call_gemini_model(api_key: str, model: str, parts: list) -> tuple[str, dict | None]:
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
    body = {"contents": [{"parts": parts}]}
    req_data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=req_data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        return "", {"error": f"Gemini request failed ({model})", "detail": detail[:2000], "status": 502}
    except Exception as exc:
        return "", {"error": f"Gemini request failed ({model})", "detail": str(exc)[:1000], "status": 502}
    try:
        obj = json.loads(raw)
    except Exception:
        return "", {"error": f"Gemini response parse failed ({model})", "detail": raw[:2000], "status": 502}
    candidates = obj.get("candidates") or []
    chunks: list[str] = []
    for c in candidates:
        content = c.get("content") or {}
        for part in content.get("parts") or []:
            txt = part.get("text", "").strip()
            if txt:
                chunks.append(txt)
    return "\n\n".join(chunks).strip(), None


def _call_gemini(api_key: str, prompt: str, trend_img: str, regime_img: str, override_model: str = "") -> tuple[str, str, dict | None]:
    explicit = bool(override_model and override_model.strip())
    model = override_model.strip() if explicit else (_get_config_value("CHART_AI_MODEL") or "gemini-2.5-flash")
    fallback = "gemini-2.0-flash"
    parts = [
        {"text": prompt},
        {"text": "Trend view screenshot:"},
        {"inline_data": {"mime_type": "image/png", "data": trend_img.split(",", 1)[-1]}} if "," in trend_img else {"text": f"[image: {trend_img[:100]}]"},
        {"text": "Regime view screenshot:"},
        {"inline_data": {"mime_type": "image/png", "data": regime_img.split(",", 1)[-1]}} if "," in regime_img else {"text": f"[image: {regime_img[:100]}]"},
    ]
    text, err = _call_gemini_model(api_key, model, parts)
    if text:
        return text, model, None
    # Only fallback if the user didn't explicitly pick a model
    if not explicit and model != fallback:
        text, err = _call_gemini_model(api_key, fallback, parts)
        if text:
            return text, fallback, None
    return "", model, err or {"error": "No explanation text returned from Gemini.", "status": 502}


def _call_gemini_text(api_key: str, prompt: str, override_model: str = "") -> tuple[str, str, dict | None]:
    explicit = bool(override_model and override_model.strip())
    model = override_model.strip() if explicit else (_get_config_value("CHART_AI_MODEL") or "gemini-2.5-flash")
    fallback = "gemini-2.0-flash"
    parts = [{"text": prompt}]
    text, err = _call_gemini_model(api_key, model, parts)
    if text:
        return text, model, None
    if not explicit and model != fallback:
        text, err = _call_gemini_model(api_key, fallback, parts)
        if text:
            return text, fallback, None
    return "", model, err or {"error": "No explanation text returned from Gemini.", "status": 502}


def _call_groq(api_key: str, prompt: str, trend_img: str, regime_img: str, override_model: str = "") -> tuple[str, str, dict | None]:
    model = override_model.strip() if override_model else (_get_config_value("CHART_AI_MODEL") or "meta-llama/llama-4-scout-17b-16e-instruct")
    content: list = [{"type": "text", "text": prompt}]
    for label, img in (("Trend view screenshot:", trend_img), ("Regime view screenshot:", regime_img)):
        content.append({"type": "text", "text": label})
        if img.startswith("data:"):
            content.append({"type": "image_url", "image_url": {"url": img}})
        else:
            content.append({"type": "text", "text": f"[image: {img[:100]}]"})
    body = {"model": model, "messages": [{"role": "user", "content": content}]}
    req_data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=req_data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "Mozilla/5.0 (compatible; finlab-chart-explain/1.0)",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        return "", model, {"error": "Groq request failed", "detail": detail[:2000], "status": 502}
    except Exception as exc:
        return "", model, {"error": "Groq request failed", "detail": str(exc)[:1000], "status": 502}
    try:
        obj = json.loads(raw)
    except Exception:
        return "", model, {"error": "Groq response parse failed", "detail": raw[:2000], "status": 502}
    text = ((obj.get("choices") or [{}])[0].get("message") or {}).get("content", "").strip()
    if not text:
        return "", model, {"error": "No explanation text returned from Groq.", "status": 502}
    return text, model, None


def _call_groq_text(api_key: str, prompt: str, override_model: str = "") -> tuple[str, str, dict | None]:
    model = override_model.strip() if override_model else (_get_config_value("CHART_AI_MODEL") or "meta-llama/llama-4-scout-17b-16e-instruct")
    body = {"model": model, "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}]}
    req_data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        "https://api.groq.com/openai/v1/chat/completions",
        data=req_data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "User-Agent": "Mozilla/5.0 (compatible; finlab-chart-explain/1.0)",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        return "", model, {"error": "Groq request failed", "detail": detail[:2000], "status": 502}
    except Exception as exc:
        return "", model, {"error": "Groq request failed", "detail": str(exc)[:1000], "status": 502}
    try:
        obj = json.loads(raw)
    except Exception:
        return "", model, {"error": "Groq response parse failed", "detail": raw[:2000], "status": 502}
    text = ((obj.get("choices") or [{}])[0].get("message") or {}).get("content", "").strip()
    if not text:
        return "", model, {"error": "No explanation text returned from Groq.", "status": 502}
    return text, model, None


def _call_anthropic(api_key: str, prompt: str, trend_img: str, regime_img: str, override_model: str = "") -> tuple[str, str, dict | None]:
    model = override_model.strip() if override_model else (_get_config_value("CHART_AI_MODEL") or "claude-3-5-sonnet-latest")
    content: list[dict] = [{"type": "text", "text": prompt}]
    for label, img in (("Trend view screenshot:", trend_img), ("Regime view screenshot:", regime_img)):
        content.append({"type": "text", "text": label})
        if img.startswith("data:") and "," in img:
            header, encoded = img.split(",", 1)
            media_type = "image/png"
            if ";base64" in header and ":" in header:
                media_type = header.split(":", 1)[1].split(";", 1)[0] or "image/png"
            content.append({
                "type": "image",
                "source": {"type": "base64", "media_type": media_type, "data": encoded},
            })
        else:
            content.append({"type": "text", "text": f"[image: {img[:100]}]"})
    body = {
        "model": model,
        "max_tokens": 1800,
        "messages": [{"role": "user", "content": content}],
    }
    req_data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=req_data,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        return "", model, {"error": "Claude request failed", "detail": detail[:2000], "status": 502}
    except Exception as exc:
        return "", model, {"error": "Claude request failed", "detail": str(exc)[:1000], "status": 502}
    try:
        obj = json.loads(raw)
    except Exception:
        return "", model, {"error": "Claude response parse failed", "detail": raw[:2000], "status": 502}
    chunks: list[str] = []
    for block in obj.get("content") or []:
        if isinstance(block, dict) and block.get("type") == "text":
            text = str(block.get("text") or "").strip()
            if text:
                chunks.append(text)
    text = "\n\n".join(chunks).strip()
    if not text:
        return "", model, {"error": "No explanation text returned from Claude.", "status": 502}
    return text, model, None


def _call_anthropic_text(api_key: str, prompt: str, override_model: str = "") -> tuple[str, str, dict | None]:
    model = override_model.strip() if override_model else (_get_config_value("CHART_AI_MODEL") or "claude-3-5-sonnet-latest")
    body = {
        "model": model,
        "max_tokens": 1200,
        "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
    }
    req_data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=req_data,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        return "", model, {"error": "Claude request failed", "detail": detail[:2000], "status": 502}
    except Exception as exc:
        return "", model, {"error": "Claude request failed", "detail": str(exc)[:1000], "status": 502}
    try:
        obj = json.loads(raw)
    except Exception:
        return "", model, {"error": "Claude response parse failed", "detail": raw[:2000], "status": 502}
    text_parts = []
    for item in obj.get("content") or []:
        txt = str(item.get("text") or "").strip()
        if txt:
            text_parts.append(txt)
    text = "\n\n".join(text_parts).strip()
    if not text:
        return "", model, {"error": "No explanation text returned from Claude.", "status": 502}
    return text, model, None


_AI_PROVIDER_CATALOG = {
    "openai": {
        "name": "OpenAI",
        "env_key": "OPENAI_API_KEY",
        "model_groups": [
            {"label": "Free / Low-cost", "models": ["gpt-4o-mini", "gpt-4.1-mini", "gpt-4.1-nano"]},
            {"label": "Standard", "models": ["gpt-4o", "gpt-4.1", "o3-mini"]},
            {"label": "Premium", "models": ["o3", "o4-mini"]},
        ],
    },
    "gemini": {
        "name": "Gemini",
        "env_key": "GEMINI_API_KEY",
        "model_groups": [
            {"label": "Free Tier", "models": ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-2.5-pro"]},
            {"label": "Preview", "models": ["gemini-3.1-flash-lite", "gemini-3.1-pro-preview"]},
        ],
    },
    "groq": {
        "name": "Groq",
        "env_key": "GROQ_API_KEY",
        "model_groups": [
            {"label": "Vision", "models": ["meta-llama/llama-4-scout-17b-16e-instruct", "meta-llama/llama-4-maverick-17b-128e-instruct"]},
            {"label": "Fast", "models": ["llama-3.3-70b-versatile", "llama3-70b-8192"]},
        ],
    },
    "anthropic": {
        "name": "Claude",
        "env_key": "ANTHROPIC_API_KEY",
        "model_groups": [
            {"label": "Haiku / Fast", "models": ["claude-3-5-haiku-latest"]},
            {"label": "Sonnet", "models": ["claude-3-5-sonnet-latest", "claude-3-7-sonnet-latest"]},
        ],
    },
}


def _load_ai_settings() -> dict:
    try:
        if AI_SETTINGS_PATH.exists():
            return json.loads(AI_SETTINGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_ai_settings(settings: dict) -> None:
    AI_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    AI_SETTINGS_PATH.write_text(json.dumps(settings, indent=2, ensure_ascii=False), encoding="utf-8")


def _get_ai_key(provider: str) -> str:
    """Return effective API key: env var takes precedence, then .env, then legacy saved settings."""
    catalog = _AI_PROVIDER_CATALOG.get(provider, {})
    env_key = _get_config_value(catalog.get("env_key", ""))
    if env_key:
        return env_key
    saved = _load_ai_settings()
    return str((saved.get("keys") or {}).get(provider, "")).strip()


@app.get("/api/ai-settings")
def api_get_ai_settings():
    saved = _load_ai_settings()
    providers = []
    for pid, cat in _AI_PROVIDER_CATALOG.items():
        env_val = _get_config_value(cat["env_key"])
        saved_val = str((saved.get("keys") or {}).get(pid, "")).strip()
        providers.append({
            "id": pid,
            "name": cat["name"],
            "model_groups": cat["model_groups"],
            "has_key": bool(env_val or saved_val),
            "key": saved_val or env_val,
            "key_source": "env" if env_val else ("saved" if saved_val else "none"),
        })
    return jsonify({
        "provider": _get_config_value("CHART_AI_PROVIDER", saved.get("provider", "")),
        "model": _get_config_value("CHART_AI_MODEL", saved.get("model", "")),
        "providers": providers,
    })


@app.post("/api/ai-settings")
def api_save_ai_settings():
    payload = request.get_json(silent=True) or {}
    provider = str(payload.get("provider", "")).strip().lower()
    model = str(payload.get("model", "")).strip()
    keys = payload.get("keys") or {}
    if not isinstance(keys, dict):
        keys = {}
    cleaned_keys = {}
    for pid in _AI_PROVIDER_CATALOG:
        val = str(keys.get(pid, "")).strip()
        if val:
            cleaned_keys[pid] = val
    settings = {"provider": provider, "model": model, "keys": cleaned_keys}
    _save_ai_settings(settings)
    _set_config_values(
        {
            "CHART_AI_PROVIDER": provider,
            "CHART_AI_MODEL": model,
            "OPENAI_API_KEY": cleaned_keys.get("openai", ""),
            "GEMINI_API_KEY": cleaned_keys.get("gemini", ""),
            "GROQ_API_KEY": cleaned_keys.get("groq", ""),
            "ANTHROPIC_API_KEY": cleaned_keys.get("anthropic", ""),
        }
    )
    return jsonify({"ok": True, "settings": settings})


@app.get("/api/config")
def api_get_config():
    saved = _load_ai_settings()
    provider = _get_config_value("CHART_AI_PROVIDER", saved.get("provider", ""))
    model = _get_config_value("CHART_AI_MODEL", saved.get("model", ""))
    llm_keys: dict[str, str] = {}
    providers: list[dict] = []
    for pid, cat in _AI_PROVIDER_CATALOG.items():
        key_val = _get_ai_key(pid)
        llm_keys[pid] = key_val
        providers.append({
            "id": pid,
            "name": cat["name"],
            "model_groups": cat["model_groups"],
            "has_key": bool(key_val),
            "env_key": cat["env_key"],
        })
    return jsonify({
        "market_data_source": _get_market_data_source(),
        "twelve_data_api_key": _get_config_value("TWELVE_DATA_API_KEY"),
        "finnhub_api_key": _get_config_value("FINNHUB_API_KEY"),
        "llm_provider": provider,
        "llm_model": model,
        "llm_keys": llm_keys,
        "providers": providers,
    })


@app.post("/api/config")
def api_save_config():
    payload = request.get_json(silent=True) or {}
    llm_keys = payload.get("llm_keys") if isinstance(payload.get("llm_keys"), dict) else {}
    updates = {
        "MARKET_DATA_SOURCE": str(payload.get("market_data_source") or "twelve_data").strip().lower(),
        "TWELVE_DATA_API_KEY": str(payload.get("twelve_data_api_key") or "").strip(),
        "FINNHUB_API_KEY": str(payload.get("finnhub_api_key") or "").strip(),
        "CHART_AI_PROVIDER": str(payload.get("llm_provider") or "").strip().lower(),
        "CHART_AI_MODEL": str(payload.get("llm_model") or "").strip(),
        "OPENAI_API_KEY": str(llm_keys.get("openai") or "").strip(),
        "GEMINI_API_KEY": str(llm_keys.get("gemini") or "").strip(),
        "GROQ_API_KEY": str(llm_keys.get("groq") or "").strip(),
        "ANTHROPIC_API_KEY": str(llm_keys.get("anthropic") or "").strip(),
    }
    _set_config_values(updates)
    settings = {
        "provider": updates["CHART_AI_PROVIDER"],
        "model": updates["CHART_AI_MODEL"],
        "keys": {
            pid: updates.get(cat["env_key"], "")
            for pid, cat in _AI_PROVIDER_CATALOG.items()
            if updates.get(cat["env_key"], "")
        },
    }
    _save_ai_settings(settings)
    return api_get_config()


@app.get("/api/ticker-search")
def api_ticker_search():
    query = str(request.args.get("q") or "").strip()
    if len(query) < 2:
        return jsonify({"items": []})
    with _upstream_usage_scope("ticker_search"):
        items = _search_twelve_symbols(query)
    return jsonify({"items": items})


@app.post("/api/chart-explain")
def api_chart_explain():
    payload = request.get_json(silent=True) or {}
    ticker = _sanitize_ticker(payload.get("ticker"))
    range_key = str(payload.get("range") or "").strip().lower()
    scale = str(payload.get("scale") or "").strip().lower()
    metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
    news_items = payload.get("news_items") if isinstance(payload.get("news_items"), list) else []
    trend_image = str(payload.get("trend_image") or "")
    regime_image = str(payload.get("regime_image") or "")

    if not ticker:
        return jsonify({"error": "ticker is required"}), 400
    if not trend_image or not regime_image:
        return jsonify({"error": "trend_image and regime_image are required"}), 400

    requested_provider = str(payload.get("provider") or "").strip().lower()
    requested_model = str(payload.get("model") or "").strip()

    # Resolve provider: use requested, else saved setting, else first available
    if not requested_provider:
        requested_provider = _get_config_value("CHART_AI_PROVIDER", _load_ai_settings().get("provider", ""))
    if not requested_provider:
        for pid in _AI_PROVIDER_CATALOG:
            if _get_ai_key(pid):
                requested_provider = pid
                break

    openai_key = _get_ai_key("openai")
    gemini_key = _get_ai_key("gemini")
    groq_key = _get_ai_key("groq")
    anthropic_key = _get_ai_key("anthropic")
    if not openai_key and not gemini_key and not groq_key and not anthropic_key:
        return jsonify(
            {
                "error": "No AI API key is configured.",
                "hint": "Open Config from the center menu and add an AI API key.",
            }
        ), 400

    window = metrics.get("window") if isinstance(metrics.get("window"), dict) else {}
    summary = metrics.get("summary") if isinstance(metrics.get("summary"), dict) else {}
    indicators = metrics.get("indicators") if isinstance(metrics.get("indicators"), dict) else {}
    patterns = metrics.get("patterns") if isinstance(metrics.get("patterns"), list) else []
    tomorrow_up_context = _build_tomorrow_up_context(ticker)
    local_forecast = _build_local_tomorrow_forecast(metrics, tomorrow_up_context)

    horizon_map = {
        "1d": "next 30 minutes to 1 trading day",
        "10days": "next 1 to 3 trading days",
        "1w": "next 1 to 3 trading days",
        "1m": "next 1 to 2 weeks",
        "3m": "next 2 to 4 weeks",
        "1y": "next 1 to 3 months",
        "max": "next 1 to 3 months",
    }
    outlook_horizon = horizon_map.get(range_key, "next 1 to 3 trading days")
    trimmed_news = []
    for item in news_items[:6]:
        if not isinstance(item, dict):
            continue
        headline = str(item.get("headline") or "").strip()
        if not headline:
            continue
        trimmed_news.append(
            {
                "headline": headline,
                "source": str(item.get("source") or "").strip(),
                "published_at": str(item.get("published_at") or "").strip(),
                "summary": str(item.get("summary") or "").strip()[:400],
            }
        )

    prompt_text = (
        "You are a technical-analysis assistant. Read both images and metrics for the same visible chart window. "
        "Explain in plain English with concise bullets and no hype. Do not give financial advice or guarantee direction. "
        "Use probabilistic language only. Do not claim certainty or a guaranteed forecast.\n\n"
        f"Ticker: {ticker}\n"
        f"Range selected: {range_key}\n"
        f"Scale: {scale}\n"
        f"Requested outlook horizon: {outlook_horizon}\n"
        f"Visible window metrics JSON: {json.dumps({'window': window, 'summary': summary, 'indicators': indicators, 'patterns': patterns, 'news': trimmed_news, 'tomorrow_up_context': tomorrow_up_context, 'local_forecast': local_forecast}, ensure_ascii=True)}\n\n"
        f"Local forecast JSON: {json.dumps(local_forecast, ensure_ascii=True)}\n\n"
        "Treat the local forecast as the primary quantitative prior. Do not replace it with a different numeric probability. "
        "Instead, explain whether the chart structure, trend/regime bands, candlestick patterns, news, and tomorrow-up context confirm it, weaken it, or put it at risk.\n\n"
        "Priority order:\n"
        "1. Explain the visible chart window.\n"
        "2. Explicitly compare Trend-view bands and Regime-view bands, and explain what each recent red/green/yellow band implies for timing vs context.\n"
        "3. Incorporate candlestick patterns if present.\n"
        "4. Incorporate the recent news headlines as possible catalysts or risks.\n"
        "5. Incorporate tomorrow-up context from earnings/statistics/fundamentals, especially earnings proximity, last EPS surprise, valuation context, beta, and revenue growth if present.\n"
        "6. Explain the local tomorrow forecast: P(up tomorrow), P(down tomorrow), confidence, and the main drivers.\n"
        "7. Then summarize the most likely NEXT scenario over the requested outlook horizon.\n\n"
        "If trend and regime conflict, call that out clearly. If news conflicts with technicals, call that out clearly.\n\n"
        "Return markdown with exactly these sections:\n"
        "1) Window Snapshot (3 bullets)\n"
        "2) Band + Pattern Interpretation (Trend + Regime + Candles) (3-6 bullets)\n"
        f"3) Next Outlook ({outlook_horizon})\n"
        "   - start with: local tomorrow forecast (P up / P down / confidence)\n"
        "   - include: bias, confidence, likely scenario, why, news impact, and tomorrow-up context impact\n"
        "4) What Confirms / What Invalidates (2 bullets each)\n"
        "5) Next Bars Checklist (numbered 1-3)\n"
    )

    key_map = {"openai": openai_key, "gemini": gemini_key, "groq": groq_key, "anthropic": anthropic_key}
    provider = requested_provider if (requested_provider in key_map and key_map[requested_provider]) else next(
        (pid for pid in ("groq", "openai", "anthropic", "gemini") if key_map[pid]), None
    )

    if provider == "groq":
        text, model, err = _call_groq(groq_key, prompt_text, trend_image, regime_image, override_model=requested_model)
    elif provider == "openai":
        text, model, err = _call_openai(openai_key, prompt_text, trend_image, regime_image, override_model=requested_model)
    elif provider == "anthropic":
        text, model, err = _call_anthropic(anthropic_key, prompt_text, trend_image, regime_image, override_model=requested_model)
    elif provider == "gemini":
        text, model, err = _call_gemini(gemini_key, prompt_text, trend_image, regime_image, override_model=requested_model)
    else:
        return jsonify({"error": f"Provider '{requested_provider}' not available — no API key configured."}), 400

    if err:
        return jsonify(err), err.get("status", 502)
    return jsonify({
        "ok": True,
        "explanation": text,
        "model": model,
        "provider": provider,
        "local_forecast": local_forecast,
        "tomorrow_up_context": tomorrow_up_context,
    })


@app.post("/api/chart-explain-followup")
def api_chart_explain_followup():
    payload = request.get_json(silent=True) or {}
    ticker = _sanitize_ticker(payload.get("ticker"))
    range_key = str(payload.get("range") or "").strip().lower()
    scale = str(payload.get("scale") or "").strip().lower()
    question = str(payload.get("question") or "").strip()
    current_read = str(payload.get("current_read") or "").strip()
    next_outlook = str(payload.get("next_outlook") or "").strip()
    local_forecast = payload.get("local_forecast") if isinstance(payload.get("local_forecast"), dict) else {}
    tomorrow_up_context = payload.get("tomorrow_up_context") if isinstance(payload.get("tomorrow_up_context"), dict) else {}
    history = payload.get("history") if isinstance(payload.get("history"), list) else []

    if not ticker:
        return jsonify({"error": "ticker is required"}), 400
    if not question:
        return jsonify({"error": "question is required"}), 400
    if not current_read and not next_outlook:
        return jsonify({"error": "current_read or next_outlook is required"}), 400

    requested_provider = str(payload.get("provider") or "").strip().lower()
    requested_model = str(payload.get("model") or "").strip()

    if not requested_provider:
        requested_provider = _get_config_value("CHART_AI_PROVIDER", _load_ai_settings().get("provider", ""))
    if not requested_provider:
        for pid in _AI_PROVIDER_CATALOG:
            if _get_ai_key(pid):
                requested_provider = pid
                break

    openai_key = _get_ai_key("openai")
    gemini_key = _get_ai_key("gemini")
    groq_key = _get_ai_key("groq")
    anthropic_key = _get_ai_key("anthropic")
    if not openai_key and not gemini_key and not groq_key and not anthropic_key:
        return jsonify(
            {
                "error": "No AI API key is configured.",
                "hint": "Open Config from the center menu and add an AI API key.",
            }
        ), 400

    cleaned_history: list[dict[str, str]] = []
    for item in history[-8:]:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        text = str(item.get("text") or "").strip()
        if role not in {"user", "assistant"} or not text:
            continue
        cleaned_history.append({"role": role, "text": text[:2000]})

    prompt_text = (
        "You are continuing a chart-analysis conversation. Answer the user's follow-up using the prior chart explanation as the primary context. "
        "Stay grounded in the existing explanation and local forecast. Be concise, clear, and specific. "
        "Do not claim certainty or provide financial advice.\n\n"
        f"Ticker: {ticker}\n"
        f"Range selected: {range_key}\n"
        f"Scale: {scale}\n\n"
        f"Current Read:\n{current_read[:6000]}\n\n"
        f"Next Outlook:\n{next_outlook[:6000]}\n\n"
        f"Local forecast JSON:\n{json.dumps(local_forecast, ensure_ascii=True)}\n\n"
        f"Tomorrow-up context JSON:\n{json.dumps(tomorrow_up_context, ensure_ascii=True)}\n\n"
        f"Recent follow-up history JSON:\n{json.dumps(cleaned_history, ensure_ascii=True)}\n\n"
        f"User follow-up question:\n{question}\n\n"
        "Answer only the user's actual question first. Do not drift into generic stock-analysis checklists. "
        "Do not add generic suggestions like 'check recent earnings', 'watch guidance', or 'monitor news' unless the user's question is explicitly about earnings, fundamentals, catalysts, or unless those topics are clearly necessary to answer. "
        "If the question is about why the stock moved or dropped, use the earnings/fundamentals context directly when it is relevant instead of giving generic advice to go look it up elsewhere. "
        "If the question is about a pattern, signal, candle, trend, or chart interpretation, answer that directly using the existing chart context and stop there. "
        "If the question asks for a next step, give a short practical answer tied to this chart only. "
        "Respond in plain English. Keep the answer to about 2-6 concise bullet points or a short paragraph when that is clearer."
    )

    key_map = {"openai": openai_key, "gemini": gemini_key, "groq": groq_key, "anthropic": anthropic_key}
    provider = requested_provider if (requested_provider in key_map and key_map[requested_provider]) else next(
        (pid for pid in ("groq", "openai", "anthropic", "gemini") if key_map[pid]), None
    )

    if provider == "groq":
        text, model, err = _call_groq_text(groq_key, prompt_text, override_model=requested_model)
    elif provider == "openai":
        text, model, err = _call_openai_text(openai_key, prompt_text, override_model=requested_model)
    elif provider == "anthropic":
        text, model, err = _call_anthropic_text(anthropic_key, prompt_text, override_model=requested_model)
    elif provider == "gemini":
        text, model, err = _call_gemini_text(gemini_key, prompt_text, override_model=requested_model)
    else:
        return jsonify({"error": f"Provider '{requested_provider}' not available — no API key configured."}), 400

    if err:
        return jsonify(err), err.get("status", 502)
    return jsonify({"ok": True, "answer": text, "model": model, "provider": provider})


@app.post("/api/backtest")
def api_backtest():
    payload = request.get_json(silent=True) or {}
    tickers_raw = payload.get("tickers") if isinstance(payload.get("tickers"), list) else []
    universe_label = str(payload.get("universe_label") or "").strip()
    tickers = []
    for item in tickers_raw:
        symbol = _sanitize_ticker(item)
        if symbol and symbol not in tickers:
            tickers.append(symbol)
    tickers = tickers[:25]
    if not tickers:
        return jsonify({"error": "At least one ticker is required."}), 400

    prob_threshold = _clamp(_safe_float(payload.get("prob_threshold")) or 0.90, 0.50, 0.99)
    confidence_threshold = _clamp(_safe_float(payload.get("confidence_threshold")) or 75.0, 0.0, 100.0)

    try:
        with _upstream_usage_scope("backtest"):
            results, summary, errors = _run_pooled_backtest(
                tickers,
                prob_threshold=prob_threshold,
                confidence_threshold=confidence_threshold,
            )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 502
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500

    if not results and errors:
        return jsonify({"error": "Backtest failed for all tickers.", "details": errors}), 502

    summary["universe_label"] = universe_label
    return jsonify(
        {
            "ok": True,
            "source": _get_market_data_source(),
            "universe_label": universe_label,
            "summary": summary,
            "results": results,
            "errors": errors,
            "thresholds": {
                "prob_threshold": prob_threshold,
                "confidence_threshold": confidence_threshold,
            },
        }
    )


@app.post("/api/scanner")
def api_scanner():
    payload = request.get_json(silent=True) or {}
    tickers_raw = payload.get("tickers") if isinstance(payload.get("tickers"), list) else []
    universe_label = str(payload.get("universe_label") or "").strip()
    tickers = []
    for item in tickers_raw:
        symbol = _sanitize_ticker(item)
        if symbol and symbol not in tickers:
            tickers.append(symbol)
    tickers = tickers[:50]
    if not tickers:
        return jsonify({"error": "At least one ticker is required."}), 400

    prob_threshold = _clamp(_safe_float(payload.get("prob_threshold")) or 0.80, 0.50, 0.99)
    confidence_threshold = _clamp(_safe_float(payload.get("confidence_threshold")) or 50.0, 0.0, 100.0)
    force_retrain = bool(payload.get("force_retrain"))

    try:
        with _upstream_usage_scope("scanner"):
            signals, summary, errors = _run_live_scanner(
                tickers,
                prob_threshold=prob_threshold,
                confidence_threshold=confidence_threshold,
                force_retrain=force_retrain,
            )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 502
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500

    summary["universe_label"] = universe_label
    return jsonify(
        {
            "ok": True,
            "source": _get_market_data_source(),
            "universe_label": universe_label,
            "summary": summary,
            "signals": signals,
            "errors": errors,
            "thresholds": {
                "prob_threshold": prob_threshold,
                "confidence_threshold": confidence_threshold,
            },
        }
    )


@app.post("/api/watchlist")
def api_watchlist():
    payload = request.get_json(silent=True) or {}
    tickers_raw = payload.get("tickers") if isinstance(payload.get("tickers"), list) else []
    universe_label = str(payload.get("universe_label") or "").strip()
    range_key = str(payload.get("range") or "1d").strip().lower()
    force_refresh = bool(payload.get("force_refresh"))
    retry_errors = bool(payload.get("retry_errors"))
    interactive = bool(payload.get("interactive", True))
    tickers: list[str] = []
    for item in tickers_raw:
        symbol = _sanitize_ticker(item)
        if symbol and symbol not in tickers:
            tickers.append(symbol)
    tickers = tickers[:50]
    if not tickers:
        return jsonify({"error": "At least one ticker is required."}), 400
    if range_key not in RANGE_MAP:
        return jsonify({"error": f"Unsupported range '{range_key}'."}), 400
    entry = _ensure_watchlist_scan(
        tickers,
        universe_label,
        range_key,
        force_refresh=force_refresh,
        retry_errors=retry_errors,
        interactive=interactive,
    )
    status_code = 200 if str(entry.get("status") or "") == "ready" else 202
    return jsonify(entry), status_code


@app.post("/api/watchlist/prewarm")
def api_watchlist_prewarm():
    payload = request.get_json(silent=True) or {}
    tickers_raw = payload.get("tickers") if isinstance(payload.get("tickers"), list) else []
    universe_label = str(payload.get("universe_label") or "").strip()
    range_key = str(payload.get("range") or "1d").strip().lower()
    tickers: list[str] = []
    for item in tickers_raw:
        symbol = _sanitize_ticker(item)
        if symbol and symbol not in tickers:
            tickers.append(symbol)
    tickers = tickers[:50]
    if not tickers:
        return jsonify({"error": "At least one ticker is required."}), 400
    if range_key not in RANGE_MAP:
        return jsonify({"error": f"Unsupported range '{range_key}'."}), 400
    entry = _ensure_watchlist_scan(tickers, universe_label, range_key, force_refresh=False, interactive=False)
    return jsonify(entry)


@app.post("/api/watchlist/status")
def api_watchlist_status():
    payload = request.get_json(silent=True) or {}
    tickers_raw = payload.get("tickers") if isinstance(payload.get("tickers"), list) else []
    range_key = str(payload.get("range") or "1d").strip().lower()
    tickers: list[str] = []
    for item in tickers_raw:
        symbol = _sanitize_ticker(item)
        if symbol and symbol not in tickers:
            tickers.append(symbol)
    tickers = tickers[:50]
    if not tickers:
        return jsonify({"error": "At least one ticker is required."}), 400
    if range_key not in RANGE_MAP:
        return jsonify({"error": f"Unsupported range '{range_key}'."}), 400
    cache_key = _watchlist_cache_key(tickers, range_key)
    entry = _get_watchlist_cache_entry(cache_key)
    if not entry:
        return jsonify({"error": "No watchlist cache found for this range/universe yet."}), 404
    status_code = 200 if str(entry.get("status") or "") == "ready" else 202
    return jsonify(entry), status_code


@app.get("/api/news")
def api_news():
    ticker = _sanitize_ticker(request.args.get("ticker"))
    if not ticker:
        return jsonify({"error": "ticker is required"}), 400
    try:
        items = _fetch_finnhub_news_cached(ticker)
        return jsonify({"ticker": ticker, "items": items})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 502


@app.post("/api/rsi-monitor/run-once")
def api_rsi_monitor_run_once():
    payload = request.get_json(silent=True) or {}
    requested = payload.get("tickers")
    if isinstance(requested, list):
        tickers = [_sanitize_ticker(x) for x in requested]
        tickers = [t for t in tickers if t]
    else:
        single = _sanitize_ticker(payload.get("ticker"))
        tickers = [single] if single else []
    if not tickers:
        tickers = ["QQQ"]
    result = _run_rsi_monitor_once(force_tickers=tickers)
    return jsonify({"ok": True, "result": result})


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}
    app.run(debug=debug, host=host, port=port, threaded=False)
