#!/usr/bin/env python3
"""
Starter script for pulling market + EPS-related data with yfinance.

Examples:
  python fetch_market_data.py
  python fetch_market_data.py --tickers NVDA QQQ --period 2y --interval 1d --save-csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from threading import Lock
from typing import Any

import pandas as pd
import yfinance as yf

_YF_DOWNLOAD_LOCK = Lock()


def fetch_price_history(ticker: str, period: str, interval: str, prepost: bool = False) -> pd.DataFrame:
    """Return OHLCV price history for one ticker."""

    def _download(symbol: str) -> pd.DataFrame:
        with _YF_DOWNLOAD_LOCK:
            return yf.download(
                tickers=symbol,
                period=period,
                interval=interval,
                prepost=prepost,
                auto_adjust=False,
                progress=False,
                group_by="ticker",
                threads=False,
            )

    def _prepare(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        # yfinance can return multi-index columns with ticker at level 0 or 1.
        if isinstance(df.columns, pd.MultiIndex):
            extracted = None
            for level in range(df.columns.nlevels):
                if symbol in df.columns.get_level_values(level):
                    extracted = df.xs(symbol, axis=1, level=level, drop_level=True)
                    break
            if extracted is not None:
                df = extracted.copy()
            else:
                df.columns = ["_".join(map(str, c)).strip() for c in df.columns.to_flat_index()]

        if df.empty:
            return df

        df = df.reset_index()
        # yfinance may return Date (daily) or Datetime (intraday) index columns.
        for candidate in ("date", "Date", "Datetime", "index"):
            if candidate in df.columns:
                if candidate != "date":
                    df = df.rename(columns={candidate: "date"})
                break

        if "date" not in df.columns:
            raise ValueError(f"Timestamp column not found. Columns={list(df.columns)}")

        return df

    candidates = [ticker]
    # Common Yahoo class-share formatting fallback, e.g. BRKB -> BRK-B / BRK.B.
    if "/" in ticker:
        candidates.append(ticker.replace("/", "-"))
    if "-" not in ticker and "." not in ticker and len(ticker) >= 4:
        candidates.append(f"{ticker[:-1]}-{ticker[-1]}")
        candidates.append(f"{ticker[:-1]}.{ticker[-1]}")

    seen = set()
    for symbol in candidates:
        if symbol in seen:
            continue
        seen.add(symbol)
        raw = _download(symbol)
        prepared = _prepare(raw, symbol)
        if not prepared.empty:
            prepared["ticker"] = symbol
            return prepared

    return pd.DataFrame()


def _safe_get(d: dict[str, Any], key: str) -> Any:
    return d.get(key) if isinstance(d, dict) else None


def fetch_fundamentals(ticker: str) -> pd.DataFrame:
    """Return a one-row DataFrame of common fundamental/EPS fields."""
    tk = yf.Ticker(ticker)
    info = tk.info or {}

    row = {
        "ticker": ticker,
        "short_name": _safe_get(info, "shortName"),
        "sector": _safe_get(info, "sector"),
        "industry": _safe_get(info, "industry"),
        "currency": _safe_get(info, "currency"),
        "market_cap": _safe_get(info, "marketCap"),
        "trailing_eps": _safe_get(info, "trailingEps"),
        "forward_eps": _safe_get(info, "forwardEps"),
        "trailing_pe": _safe_get(info, "trailingPE"),
        "forward_pe": _safe_get(info, "forwardPE"),
        "price_to_book": _safe_get(info, "priceToBook"),
        "dividend_yield": _safe_get(info, "dividendYield"),
    }

    return pd.DataFrame([row])


def fetch_earnings_dates(ticker: str, limit: int = 8) -> pd.DataFrame:
    """Return recent/pending earnings dates + EPS estimates/actuals when available."""
    tk = yf.Ticker(ticker)
    try:
        df = tk.get_earnings_dates(limit=limit)
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index().rename(columns={"Earnings Date": "earnings_date"})
    df["ticker"] = ticker
    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch historical + EPS-related data with yfinance.")
    parser.add_argument("--tickers", nargs="+", default=["NVDA", "QQQ"], help="Ticker list.")
    parser.add_argument("--period", default="1y", help="History window, e.g. 6mo, 1y, 5y, max.")
    parser.add_argument("--interval", default="1d", help="Data interval, e.g. 1d, 1wk, 1mo.")
    parser.add_argument("--save-csv", action="store_true", help="Save results into ./data/")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tickers = [t.upper() for t in args.tickers]

    all_history: list[pd.DataFrame] = []
    all_fundamentals: list[pd.DataFrame] = []
    all_earnings_dates: list[pd.DataFrame] = []

    for ticker in tickers:
        history_df = fetch_price_history(ticker, args.period, args.interval)
        fundamentals_df = fetch_fundamentals(ticker)
        earnings_dates_df = fetch_earnings_dates(ticker)

        all_history.append(history_df)
        all_fundamentals.append(fundamentals_df)
        all_earnings_dates.append(earnings_dates_df)

    history = pd.concat(all_history, ignore_index=True) if all_history else pd.DataFrame()
    fundamentals = pd.concat(all_fundamentals, ignore_index=True) if all_fundamentals else pd.DataFrame()
    earnings_dates = (
        pd.concat(all_earnings_dates, ignore_index=True) if all_earnings_dates else pd.DataFrame()
    )

    print("\n=== Price/Volume History (sample) ===")
    print(history.head(10).to_string(index=False) if not history.empty else "No history data.")

    print("\n=== Fundamentals (includes EPS fields) ===")
    print(fundamentals.to_string(index=False) if not fundamentals.empty else "No fundamentals data.")

    print("\n=== Earnings Dates / EPS surprises (if available) ===")
    print(earnings_dates.head(10).to_string(index=False) if not earnings_dates.empty else "No earnings dates data.")

    if args.save_csv:
        out_dir = Path("data")
        out_dir.mkdir(parents=True, exist_ok=True)
        history.to_csv(out_dir / "history.csv", index=False)
        fundamentals.to_csv(out_dir / "fundamentals.csv", index=False)
        earnings_dates.to_csv(out_dir / "earnings_dates.csv", index=False)
        print(f"\nSaved CSV files under: {out_dir.resolve()}")


if __name__ == "__main__":
    main()
