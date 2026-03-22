from __future__ import annotations

import csv
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from flask import Flask, g, jsonify, render_template, request

from fetch_market_data import diagnose_source_failure, fetch_price_history_by_source

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

# Map UI ranges to yfinance period/interval values.
RANGE_MAP: Dict[str, Tuple[str, str]] = {
    "1d": ("1d", "1m"),
    "1w": ("10d", "5m"),
    "1m": ("1mo", "15m"),
    "3m": ("3mo", "1h"),
    "1y": ("1y", "1d"),
    "max": ("max", "1d"),
}

BASE_DIR = Path(__file__).resolve().parent
PORTFOLIO_DIR = BASE_DIR / "data" / "imported_portfolios"
PORTFOLIO_FILES_DIR = PORTFOLIO_DIR / "files"
PORTFOLIO_CONFIG_PATH = PORTFOLIO_DIR / "portfolios.json"

LOG_DIR = BASE_DIR / "data" / "logs"
SERVER_LOG_PATH = LOG_DIR / "server.log"
CLIENT_ACTION_LOG_PATH = LOG_DIR / "client_actions.jsonl"
RSI_MONITOR_LOG_PATH = LOG_DIR / "rsi_monitor.jsonl"

CATEGORY_DIR = BASE_DIR / "data" / "categories"
CATEGORY_CONFIG_PATH = CATEGORY_DIR / "categories.json"

AI_SETTINGS_PATH = BASE_DIR / "data" / "ai_settings.json"

CACHE_DIR = BASE_DIR / "data" / "cache"
CACHE_DB_PATH = CACHE_DIR / "market_data.db"
CACHE_MAX_BYTES = 50 * 1024 * 1024  # 50 MB
CACHE_MAX_AGE_SEC = 7 * 24 * 3600   # 7 days

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


def _fetch_finnhub_news_cached(ticker: str, day_window: int = 10, ttl_sec: int = 300) -> list[dict]:
    symbol = _sanitize_ticker(ticker)
    if not symbol:
        raise ValueError("Ticker is required.")

    api_key = (os.getenv("FINNHUB_API_KEY") or "").strip()
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
    df, _ = fetch_price_history_by_source(
        ticker=ticker,
        period="1d",
        interval="1m",
        prepost=True,
        source="twelve_data",
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
        }
    )


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
    normalized_name = name[:80].strip().lower()
    kept_items = []
    for item in items:
        item_name = str(item.get("name") or "").strip().lower()
        if item_name == normalized_name:
            old_csv_rel = str(item.get("csv_path") or "").strip()
            if old_csv_rel:
                try:
                    (BASE_DIR / old_csv_rel).unlink(missing_ok=True)
                except Exception:
                    pass
            continue
        kept_items.append(item)

    items = kept_items
    items.insert(
        0,
        {
            "id": portfolio_id,
            "name": name[:80],
            "csv_path": str(stored_path.relative_to(BASE_DIR)),
            "collapsed": False,
            "selected_keys": [],
            "removed_keys": [],
            "selected_symbols": [],
            "removed_symbols": [],
            "created_at": datetime.now(timezone.utc).isoformat(),
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
    source = "twelve_data"
    force_refresh = (request.args.get("refresh") or "").strip().lower() in {"1", "true", "yes", "on"}

    if not ticker:
        return jsonify({"error": "Ticker is required."}), 400

    if range_key not in RANGE_MAP:
        return jsonify({"error": f"Unsupported range '{range_key}'."}), 400
    period, interval = RANGE_MAP[range_key]
    use_prepost = range_key == "1d"

    try:
        ttl_history_sec = 0 if force_refresh else 300
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

        points = _to_chart_points(df)
        if range_key == "1d":
            points = _filter_regular_session_points(points)
        if not points:
            extra_hint = ""
            if not os.getenv("TWELVE_DATA_API_KEY", "").strip():
                extra_hint = " TWELVE_DATA_API_KEY is not set in this server process."
            diagnosis = diagnose_source_failure(ticker=ticker, period=period, interval=interval, source=source)
            if diagnosis:
                extra_hint = f"{extra_hint} {diagnosis}".strip()
            return jsonify(
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
                }
            ), 502
        ma_windows = {"ma50": 50, "ma200": 200}
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
            ma_points = _to_moving_average_points(daily_df, ma_windows)
        except Exception:
            ma_points = {}
        return jsonify(
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
            }
        )
    except Exception as exc:  # pragma: no cover
        return jsonify(
            {
                "error": str(exc),
                "type": exc.__class__.__name__,
                "ticker": ticker,
                "range": range_key,
            }
        ), 500


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
    model = override_model.strip() if override_model else (os.getenv("OPENAI_CHART_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini")
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
    model = override_model.strip() if explicit else (os.getenv("GEMINI_CHART_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash")
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


def _call_groq(api_key: str, prompt: str, trend_img: str, regime_img: str, override_model: str = "") -> tuple[str, str, dict | None]:
    model = override_model.strip() if override_model else (os.getenv("GROQ_CHART_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct").strip() or "meta-llama/llama-4-scout-17b-16e-instruct")
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
    """Return effective API key: env var takes precedence, then saved settings."""
    catalog = _AI_PROVIDER_CATALOG.get(provider, {})
    env_key = os.getenv(catalog.get("env_key", ""), "").strip()
    if env_key:
        return env_key
    saved = _load_ai_settings()
    return str((saved.get("keys") or {}).get(provider, "")).strip()


@app.get("/api/ai-settings")
def api_get_ai_settings():
    saved = _load_ai_settings()
    providers = []
    for pid, cat in _AI_PROVIDER_CATALOG.items():
        env_val = os.getenv(cat["env_key"], "").strip()
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
        "provider": saved.get("provider", ""),
        "model": saved.get("model", ""),
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
    return jsonify({"ok": True, "settings": settings})


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
        saved = _load_ai_settings()
        requested_provider = saved.get("provider", "")
    if not requested_provider:
        for pid in _AI_PROVIDER_CATALOG:
            if _get_ai_key(pid):
                requested_provider = pid
                break

    openai_key = _get_ai_key("openai")
    gemini_key = _get_ai_key("gemini")
    groq_key = _get_ai_key("groq")
    if not openai_key and not gemini_key and not groq_key:
        return jsonify(
            {
                "error": "No AI API key is configured.",
                "hint": "Open the AI Explain panel and add an API key in settings.",
            }
        ), 400

    window = metrics.get("window") if isinstance(metrics.get("window"), dict) else {}
    summary = metrics.get("summary") if isinstance(metrics.get("summary"), dict) else {}
    indicators = metrics.get("indicators") if isinstance(metrics.get("indicators"), dict) else {}
    patterns = metrics.get("patterns") if isinstance(metrics.get("patterns"), list) else []

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
        f"Visible window metrics JSON: {json.dumps({'window': window, 'summary': summary, 'indicators': indicators, 'patterns': patterns, 'news': trimmed_news}, ensure_ascii=True)}\n\n"
        "Priority order:\n"
        "1. Explain the visible chart window.\n"
        "2. Explicitly compare Trend-view bands and Regime-view bands, and explain what each recent red/green/yellow band implies for timing vs context.\n"
        "3. Incorporate candlestick patterns if present.\n"
        "4. Incorporate the recent news headlines as possible catalysts or risks.\n"
        "5. Then summarize the most likely NEXT scenario over the requested outlook horizon.\n\n"
        "If trend and regime conflict, call that out clearly. If news conflicts with technicals, call that out clearly.\n\n"
        "Return markdown with exactly these sections:\n"
        "1) Window Snapshot (3 bullets)\n"
        "2) Band + Pattern Interpretation (Trend + Regime + Candles) (3-6 bullets)\n"
        f"3) Next Outlook ({outlook_horizon})\n"
        "   - include: bias, confidence, likely scenario, why, and news impact\n"
        "4) What Confirms / What Invalidates (2 bullets each)\n"
        "5) Next Bars Checklist (numbered 1-3)\n"
    )

    key_map = {"openai": openai_key, "gemini": gemini_key, "groq": groq_key}
    provider = requested_provider if (requested_provider in key_map and key_map[requested_provider]) else next(
        (pid for pid in ("groq", "openai", "gemini") if key_map[pid]), None
    )

    if provider == "groq":
        text, model, err = _call_groq(groq_key, prompt_text, trend_image, regime_image, override_model=requested_model)
    elif provider == "openai":
        text, model, err = _call_openai(openai_key, prompt_text, trend_image, regime_image, override_model=requested_model)
    elif provider == "gemini":
        text, model, err = _call_gemini(gemini_key, prompt_text, trend_image, regime_image, override_model=requested_model)
    else:
        return jsonify({"error": f"Provider '{requested_provider}' not available — no API key configured."}), 400

    if err:
        return jsonify(err), err.get("status", 502)
    return jsonify({"ok": True, "explanation": text, "model": model, "provider": provider})


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
