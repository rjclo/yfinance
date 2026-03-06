#!/usr/bin/env python3
"""
Market data helpers using Twelve Data only.
"""

from __future__ import annotations

import argparse
import io
import os
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import urllib3
from urllib3.exceptions import InsecureRequestWarning

_HTTP_TIMEOUT_SEC = float(os.getenv("MARKET_HTTP_TIMEOUT_SEC", "15"))
_MARKET_INSECURE_SSL = os.getenv("MARKET_INSECURE_SSL", "0").strip().lower() in {"1", "true", "yes", "on"}
if _MARKET_INSECURE_SSL:
    urllib3.disable_warnings(InsecureRequestWarning)


def _http_verify() -> bool:
    return not _MARKET_INSECURE_SSL


def _get_twelve_data_api_key() -> str:
    return os.getenv("TWELVE_DATA_API_KEY", "").strip()


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
        if resp.status_code != 200:
            return pd.DataFrame()
        payload = resp.json() or {}
    except Exception:
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
    del prepost, source  # Twelve Data path only.
    df = _fetch_twelve_data(ticker=ticker, period=period, interval=interval)
    if not df.empty:
        return df, "twelve_data"
    return pd.DataFrame(), "none"


def diagnose_source_failure(ticker: str, period: str, interval: str, source: str = "twelve_data") -> str:
    del period, source
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
