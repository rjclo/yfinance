#!/usr/bin/env python3
"""
Market data helpers using a provider abstraction with Twelve Data as the
current upstream implementation.
"""

from __future__ import annotations

import argparse
import contextvars
import io
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning

_HTTP_TIMEOUT_SEC = float(os.getenv("MARKET_HTTP_TIMEOUT_SEC", "15"))
_MARKET_INSECURE_SSL = os.getenv("MARKET_INSECURE_SSL", "0").strip().lower() in {"1", "true", "yes", "on"}
_BASE_DIR = Path(__file__).resolve().parent
_DOTENV_PATH = _BASE_DIR / ".env"
_UPSTREAM_API_LOG_PATH = _BASE_DIR / "data" / "logs" / "upstream_api_calls.jsonl"
_UPSTREAM_USAGE_CONTEXT = contextvars.ContextVar("upstream_usage_context", default="other")
if _MARKET_INSECURE_SSL:
    urllib3.disable_warnings(InsecureRequestWarning)


def _append_upstream_api_log(entry: dict) -> None:
    try:
        _UPSTREAM_API_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _UPSTREAM_API_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=True) + "\n")
    except Exception:
        pass


def set_upstream_usage_context(value: str):
    label = str(value or "other").strip().lower() or "other"
    return _UPSTREAM_USAGE_CONTEXT.set(label)


def reset_upstream_usage_context(token) -> None:
    try:
        _UPSTREAM_USAGE_CONTEXT.reset(token)
    except Exception:
        pass


def get_upstream_usage_context() -> str:
    try:
        return str(_UPSTREAM_USAGE_CONTEXT.get() or "other")
    except Exception:
        return "other"


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


def _get_config_value(key: str, default: str = "") -> str:
    env_val = os.getenv(key, "").strip()
    if env_val:
        return env_val
    try:
        if _DOTENV_PATH.exists():
            file_values = _parse_dotenv_text(_DOTENV_PATH.read_text(encoding="utf-8"))
            return (file_values.get(key) or "").strip() or default
    except Exception:
        pass
    return default


def _http_verify() -> bool:
    return not _MARKET_INSECURE_SSL


def _get_twelve_data_api_key() -> str:
    return _get_config_value("TWELVE_DATA_API_KEY")


def _get_market_data_source(default: str = "twelve_data") -> str:
    source = _get_config_value("MARKET_DATA_SOURCE", default).strip().lower()
    return source if source in {"twelve_data"} else default


def _period_to_days(period: str) -> int | None:
    text = (period or "").strip().lower()
    if text == "max":
        return None
    if text.endswith("d"):
        return int(text[:-1] or "0")
    if text.endswith("mo"):
        return int(text[:-2] or "0") * 30
    if text.endswith("y"):
        return int(text[:-1] or "0") * 365
    return None


def _interval_to_twelve(interval: str) -> str | None:
    m = {
        "1m": "1min",
        "5m": "5min",
        "10m": "10min",
        "15m": "15min",
        "1h": "1h",
        "1d": "1day",
    }
    return m.get((interval or "").strip().lower())


def _fetch_twelve_data(ticker: str, period: str, interval: str) -> pd.DataFrame:
    api_key = _get_twelve_data_api_key()
    if not api_key:
        return pd.DataFrame()

    twelve_interval = _interval_to_twelve(interval)
    if not twelve_interval:
        return pd.DataFrame()

    params = {
        "symbol": ticker,
        "interval": twelve_interval,
        "apikey": api_key,
        "outputsize": 5000,
        "format": "JSON",
    }

    try:
        resp = requests.get(
            "https://api.twelvedata.com/time_series",
            params=params,
            timeout=_HTTP_TIMEOUT_SEC,
            verify=_http_verify(),
        )
        _append_upstream_api_log({
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "provider": "twelve_data",
            "endpoint": "time_series",
            "symbol": ticker,
            "period": period,
            "interval": interval,
            "http_status": int(resp.status_code),
            "usage_context": get_upstream_usage_context(),
        })
        if resp.status_code != 200:
            return pd.DataFrame()
        payload = resp.json() or {}
    except Exception:
        _append_upstream_api_log({
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "provider": "twelve_data",
            "endpoint": "time_series",
            "symbol": ticker,
            "period": period,
            "interval": interval,
            "http_status": None,
            "error": "request_exception",
            "usage_context": get_upstream_usage_context(),
        })
        return pd.DataFrame()

    values = payload.get("values")
    if not isinstance(values, list) or not values:
        return pd.DataFrame()

    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    source_tz = str(meta.get("exchange_timezone") or meta.get("timezone") or "UTC").strip() or "UTC"

    rows = []
    for v in values:
        if not isinstance(v, dict):
            continue
        rows.append(
            {
                "date": v.get("datetime"),
                "Open": v.get("open"),
                "High": v.get("high"),
                "Low": v.get("low"),
                "Close": v.get("close"),
                "Volume": v.get("volume"),
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    parsed = pd.to_datetime(df["date"], errors="coerce")
    if parsed.dt.tz is None:
        try:
            parsed = parsed.dt.tz_localize(ZoneInfo(source_tz), ambiguous="infer", nonexistent="shift_forward")
        except Exception:
            parsed = parsed.dt.tz_localize("UTC")
    df["date"] = parsed.dt.tz_convert("UTC")
    for c in ("Open", "High", "Low", "Close", "Volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["date", "Close"]).sort_values("date")

    keep_days = _period_to_days(period)
    if keep_days is not None and keep_days > 0 and not df.empty:
        cutoff = df["date"].max() - pd.Timedelta(days=keep_days + 2)
        df = df[df["date"] >= cutoff]

    if df.empty:
        return pd.DataFrame()

    df["ticker"] = ticker
    return df.reset_index(drop=True)


def fetch_price_history_by_source(
    ticker: str,
    period: str,
    interval: str,
    prepost: bool = False,
    source: str = "twelve_data",
) -> tuple[pd.DataFrame, str]:
    del prepost
    provider = (source or _get_market_data_source()).strip().lower()
    if provider != "twelve_data":
        return pd.DataFrame(), provider or "unknown"
    df = _fetch_twelve_data(ticker=ticker, period=period, interval=interval)
    if not df.empty:
        return df, "twelve_data"
    return pd.DataFrame(), "none"


def diagnose_source_failure(ticker: str, period: str, interval: str, source: str = "twelve_data") -> str:
    del period
    provider = (source or _get_market_data_source()).strip().lower()
    if provider != "twelve_data":
        return f"Unsupported market data source: {provider}"

    api_key = _get_twelve_data_api_key()
    if not api_key:
        return "TWELVE_DATA_API_KEY is not set in server process."

    twelve_interval = _interval_to_twelve(interval)
    if twelve_interval is None:
        return f"Twelve Data interval not supported: {interval}"

    params = {
        "symbol": ticker,
        "interval": twelve_interval,
        "apikey": api_key,
        "outputsize": 2,
        "format": "JSON",
    }
    try:
        r = requests.get(
            "https://api.twelvedata.com/time_series",
            params=params,
            timeout=_HTTP_TIMEOUT_SEC,
            verify=_http_verify(),
        )
        _append_upstream_api_log({
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "provider": "twelve_data",
            "endpoint": "time_series",
            "symbol": ticker,
            "period": "",
            "interval": interval,
            "http_status": int(r.status_code),
            "diagnostic": True,
            "usage_context": get_upstream_usage_context(),
        })
        if r.status_code != 200:
            return f"Twelve Data HTTP {r.status_code}."
        payload = r.json() or {}
        if isinstance(payload, dict) and not isinstance(payload.get("values"), list):
            code = str(payload.get("code") or "")
            msg = str(payload.get("message") or payload.get("status") or "no values")
            return f"Twelve Data: {code} {msg}".strip()
    except Exception as exc:
        return f"Twelve Data request failed: {exc.__class__.__name__}"

    return "Twelve Data returned no values for this request."


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch historical data from Twelve Data.")
    parser.add_argument("--tickers", nargs="+", default=["QQQ"], help="Ticker list.")
    parser.add_argument("--period", default="1mo", help="History window, e.g. 1d, 1mo, 1y, max.")
    parser.add_argument("--interval", default="15m", help="Data interval, e.g. 1m, 5m, 15m, 1h, 1d.")
    parser.add_argument("--save-csv", action="store_true", help="Save results into ./data/")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tickers = [t.upper() for t in args.tickers]

    all_history: list[pd.DataFrame] = []
    for ticker in tickers:
        df, _ = fetch_price_history_by_source(ticker=ticker, period=args.period, interval=args.interval)
        all_history.append(df)

    history = pd.concat(all_history, ignore_index=True) if all_history else pd.DataFrame()
    print(history.head(10).to_string(index=False) if not history.empty else "No history data.")

    if args.save_csv:
        out_dir = Path("data")
        out_dir.mkdir(parents=True, exist_ok=True)
        history.to_csv(out_dir / "history.csv", index=False)
        print(f"Saved CSV under: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
