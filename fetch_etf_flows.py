#!/usr/bin/env python3
"""
Bitcoin ETF Flow Data Fetcher
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Uses SoSoValue's free API for Bitcoin spot ETF daily flow data.

Setup (one-time):
  1. Go to https://sosovalue.com/developer
  2. Sign up and get a free API key
  3. In your GitHub repo: Settings → Secrets → Add: SOSOVALUE_API_KEY
  4. Run the workflow manually to test

The free tier allows 20 calls/min — more than enough for daily updates.
"""

import json
import os
import sys
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError


# ─── Configuration ───────────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
FLOWS_FILE = os.path.join(DATA_DIR, "etf_flows.json")

API_KEY = os.environ.get("SOSOVALUE_API_KEY", "")
BASE_URL = "https://openapi.sosovalue.com"

ETF_TICKERS = [
    "IBIT", "FBTC", "GBTC", "BTC", "BITB",
    "ARKB", "HODL", "BTCO", "BRRR", "EZBC", "BTCW", "DEFI"
]

ETF_INFO = {
    "IBIT": {"name": "iShares Bitcoin Trust", "issuer": "BlackRock"},
    "FBTC": {"name": "Wise Origin Bitcoin Fund", "issuer": "Fidelity"},
    "GBTC": {"name": "Grayscale Bitcoin Trust", "issuer": "Grayscale"},
    "BTC":  {"name": "Grayscale Bitcoin Mini Trust", "issuer": "Grayscale"},
    "BITB": {"name": "Bitwise Bitcoin ETF", "issuer": "Bitwise"},
    "ARKB": {"name": "ARK 21Shares Bitcoin ETF", "issuer": "ARK/21Shares"},
    "HODL": {"name": "VanEck Bitcoin ETF", "issuer": "VanEck"},
    "BTCO": {"name": "Invesco Galaxy Bitcoin ETF", "issuer": "Invesco"},
    "BRRR": {"name": "Valkyrie Bitcoin Fund", "issuer": "CoinShares"},
    "EZBC": {"name": "Franklin Bitcoin ETF", "issuer": "Franklin Templeton"},
    "BTCW": {"name": "WisdomTree Bitcoin Fund", "issuer": "WisdomTree"},
    "DEFI": {"name": "Hashdex Bitcoin ETF", "issuer": "Hashdex"},
}

# Map various ETF names from API responses to our standardized tickers
TICKER_ALIASES = {}
for ticker, info in ETF_INFO.items():
    TICKER_ALIASES[ticker] = ticker
    TICKER_ALIASES[ticker.lower()] = ticker
    TICKER_ALIASES[info["name"]] = ticker
    TICKER_ALIASES[info["name"].lower()] = ticker


# ─── API Helpers ─────────────────────────────────────────────────────────────
def api_get(endpoint: str, params: dict = None) -> dict:
    """Make GET request to SoSoValue API."""
    url = f"{BASE_URL}{endpoint}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
        url += f"?{qs}"

    req = Request(url, headers={
        "x-soso-api-key": API_KEY,
        "Accept": "application/json",
    })

    print(f"[API] GET {endpoint}")
    with urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8")
        data = json.loads(body)

    if data.get("code") != 0:
        raise Exception(f"API error (code={data.get('code')}): {data.get('msg')}")

    return data.get("data")


def resolve_ticker(name: str) -> str | None:
    """Resolve ETF name to our standardized ticker."""
    if not name:
        return None

    # Exact match
    if name in TICKER_ALIASES:
        return TICKER_ALIASES[name]

    # Case-insensitive
    lower = name.lower().strip()
    if lower in TICKER_ALIASES:
        return TICKER_ALIASES[lower]

    # Partial match (e.g. "iShares Bitcoin Trust ETF" → IBIT)
    for alias, ticker in TICKER_ALIASES.items():
        if alias.lower() in lower or lower in alias.lower():
            return ticker

    # Extract ticker from parentheses: "Something (IBIT)"
    import re
    m = re.search(r'\(([A-Z]{3,5})\)', name)
    if m and m.group(1) in ETF_TICKERS:
        return m.group(1)

    print(f"  [WARN] Unknown ETF name: '{name}'")
    return None


def to_millions(val) -> float:
    """Convert a value to millions USD. Auto-detects if already in millions."""
    if val is None:
        return 0.0
    f = float(val)
    # If abs value > 50,000 assume it's in raw USD, convert to millions
    # Typical daily ETF flow ranges: -1000M to +1000M
    if abs(f) > 50_000:
        return round(f / 1_000_000, 1)
    return round(f, 1)


# ─── Fetch ETF Flow History ─────────────────────────────────────────────────
def fetch_etf_flows() -> list[dict]:
    """
    Fetch Bitcoin ETF flow history from SoSoValue.
    Tries multiple known endpoint patterns.
    """
    # SoSoValue API endpoint for ETF inflow history
    # Try documented endpoints (structure may vary)
    endpoints_to_try = [
        ("/api/v1/etf/history/inflow", {"currency": "BTC"}),
        ("/api/v1/etf/historical/inflow", {"currency": "BTC"}),
        ("/api/v1/etf/inflow/history", {"currency": "BTC"}),
        ("/api/v1/etf/btc/history", {}),
    ]

    data = None
    used_endpoint = None

    for endpoint, params in endpoints_to_try:
        try:
            data = api_get(endpoint, params)
            used_endpoint = endpoint
            print(f"[OK] Successfully called {endpoint}")
            break
        except HTTPError as e:
            if e.code == 404:
                print(f"  [SKIP] {endpoint} → 404 Not Found")
                continue
            raise
        except Exception as e:
            print(f"  [SKIP] {endpoint} → {e}")
            continue

    if data is None:
        print("[ERROR] No working API endpoint found.")
        print("  The SoSoValue API structure may have changed.")
        print("  Check: https://sosovalue.gitbook.io/soso-value-api-doc/")
        return []

    # Debug: print data structure
    if isinstance(data, dict):
        print(f"[DEBUG] Response keys: {list(data.keys())[:10]}")
    elif isinstance(data, list):
        print(f"[DEBUG] Response is a list with {len(data)} items")
        if data:
            print(f"[DEBUG] First item keys: {list(data[0].keys()) if isinstance(data[0], dict) else type(data[0])}")

    # Parse the response into our standard format
    records = parse_sosovalue_response(data)

    if records:
        print(f"[OK] Parsed {len(records)} daily flow records (endpoint: {used_endpoint})")
    return records


def parse_sosovalue_response(data) -> list[dict]:
    """
    Parse SoSoValue API response into standardized records.
    Handles various response structures flexibly.
    """
    records = []

    # Determine the list of daily entries
    daily_list = []
    if isinstance(data, list):
        daily_list = data
    elif isinstance(data, dict):
        # Try common keys
        for key in ["list", "dataList", "items", "history", "data", "flows"]:
            if key in data and isinstance(data[key], list):
                daily_list = data[key]
                break

    if not daily_list:
        print(f"[WARN] Could not find daily flow list in response")
        print(f"[DEBUG] Full response preview: {json.dumps(data, default=str)[:1000]}")
        return []

    for entry in daily_list:
        if not isinstance(entry, dict):
            continue

        # ── Parse date ──
        date_iso = None
        for date_key in ["date", "tradingDate", "dataDate", "day", "time", "timestamp"]:
            if date_key in entry:
                raw = entry[date_key]
                if isinstance(raw, (int, float)) and raw > 1_000_000_000:
                    # Unix timestamp (seconds or milliseconds)
                    ts = raw / 1000 if raw > 1_000_000_000_000 else raw
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                    date_iso = dt.strftime("%Y-%m-%d")
                elif isinstance(raw, str) and len(raw) >= 10:
                    date_iso = raw[:10]
                break

        if not date_iso:
            continue

        # ── Parse total ──
        total = 0.0
        for total_key in ["totalNetInflow", "netInflow", "total", "totalFlow", "netFlow", "dailyNetInflow"]:
            if total_key in entry and entry[total_key] is not None:
                total = to_millions(entry[total_key])
                break

        # ── Parse per-ETF flows ──
        flows = {t: 0.0 for t in ETF_TICKERS}
        etf_total = 0.0

        # Look for ETF breakdown list
        for list_key in ["etfList", "list", "etfs", "funds", "tickers", "details"]:
            if list_key in entry and isinstance(entry[list_key], list):
                for etf in entry[list_key]:
                    if not isinstance(etf, dict):
                        continue

                    # Find ticker
                    etf_name = ""
                    for name_key in ["name", "ticker", "symbol", "etfName", "fundName"]:
                        if name_key in etf:
                            etf_name = str(etf[name_key])
                            break

                    ticker = resolve_ticker(etf_name)
                    if not ticker:
                        continue

                    # Find flow value
                    for val_key in ["netInflow", "change", "flow", "dailyChange", "netFlow", "value"]:
                        if val_key in etf and etf[val_key] is not None:
                            flows[ticker] = to_millions(etf[val_key])
                            etf_total += flows[ticker]
                            break
                break

        # Use ETF-level total if overall total is missing
        if total == 0 and etf_total != 0:
            total = round(etf_total, 1)

        records.append({
            "date": date_iso,
            "flows": flows,
            "total": total,
        })

    records.sort(key=lambda x: x["date"])
    return records


# ─── Data Storage ────────────────────────────────────────────────────────────
def load_existing() -> dict:
    """Load existing data from JSON file."""
    if os.path.exists(FLOWS_FILE):
        with open(FLOWS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "metadata": {
            "description": "Bitcoin Spot ETF Daily Net Flows (US$M)",
            "source": "SoSoValue API",
            "etf_info": ETF_INFO,
            "tickers": ETF_TICKERS,
            "last_updated": None,
        },
        "daily_flows": [],
    }


def merge_and_save(existing: dict, new_records: list[dict]):
    """Merge new records and save."""
    existing_map = {r["date"]: i for i, r in enumerate(existing["daily_flows"])}
    added = updated = 0

    for rec in new_records:
        if rec["date"] in existing_map:
            idx = existing_map[rec["date"]]
            new_has_detail = any(v != 0 for v in rec["flows"].values())
            if new_has_detail or rec["total"] != 0:
                existing["daily_flows"][idx] = rec
                updated += 1
        else:
            existing["daily_flows"].append(rec)
            added += 1

    existing["daily_flows"].sort(key=lambda x: x["date"])
    existing["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(FLOWS_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

    print(f"[INFO] +{added} new, ~{updated} updated → Total: {len(existing['daily_flows'])} records")
    print(f"[OK] Saved to {FLOWS_FILE}")


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    if not API_KEY:
        print("=" * 60)
        print("  SOSOVALUE_API_KEY is not set!")
        print()
        print("  Setup steps:")
        print("  1. Visit https://sosovalue.com/developer")
        print("  2. Sign up for a free API key")
        print("  3. GitHub → Settings → Secrets → New:")
        print("     Name:  SOSOVALUE_API_KEY")
        print("     Value: your-api-key-here")
        print("  4. Re-run this workflow")
        print("=" * 60)
        sys.exit(1)

    try:
        records = fetch_etf_flows()
        if not records:
            print("[WARN] No records fetched")
            sys.exit(1)

        existing = load_existing()
        merge_and_save(existing, records)

    except HTTPError as e:
        print(f"[ERROR] HTTP {e.code}: {e.reason}")
        if e.code == 401:
            print("  → Invalid API key")
        elif e.code == 429:
            print("  → Rate limited (wait 1 min)")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
