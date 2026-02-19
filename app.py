from __future__ import annotations

import csv
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
from flask import Flask, g, jsonify, render_template, request

from fetch_market_data import fetch_price_history

app = Flask(__name__)

# Map UI ranges to yfinance period/interval values.
RANGE_MAP: Dict[str, Tuple[str, str]] = {
    "1d": ("1d", "1m"),
    "1w": ("5d", "5m"),
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

CATEGORY_DIR = BASE_DIR / "data" / "categories"
CATEGORY_CONFIG_PATH = CATEGORY_DIR / "categories.json"


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
                s = _sanitize_ticker(h.get("symbol"))
                acct = _sanitize_account(h.get("account")) if h.get("account") else ""
                key = _holding_key(s, acct)
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

    if not ticker:
        return jsonify({"error": "Ticker is required."}), 400

    if range_key not in RANGE_MAP:
        return jsonify({"error": f"Unsupported range '{range_key}'."}), 400

    period, interval = RANGE_MAP[range_key]
    use_prepost = range_key == "1d"

    try:
        df = fetch_price_history(ticker=ticker, period=period, interval=interval, prepost=use_prepost)
        if df.empty and range_key == "1d":
            df = fetch_price_history(ticker=ticker, period="1d", interval="5m", prepost=True)

        points = _to_chart_points(df)
        ma_windows = {"ma30": 30, "ma90": 90, "ma1y": 252}
        ma_points: dict[str, list[dict]] = {}
        try:
            daily_df = fetch_price_history(ticker=ticker, period="2y", interval="1d")
            ma_points = _to_moving_average_points(daily_df, ma_windows)
        except Exception:
            ma_points = {}
        return jsonify(
            {
                "ticker": ticker,
                "range": range_key,
                "period": period,
                "interval": interval,
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


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "0").strip().lower() in {"1", "true", "yes", "on"}
    app.run(debug=debug, host=host, port=port, threaded=False)
