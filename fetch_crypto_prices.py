#!/usr/bin/env python3
"""
Crypto Price Fetcher (CoinGecko)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Fetches ETH & SOL daily prices using CoinGecko free API.
Outputs JSON in same format as btc_price.json.

Usage:
  python3 fetch_crypto_prices.py          # fetch all (eth, sol)
  python3 fetch_crypto_prices.py eth      # fetch ETH only
  python3 fetch_crypto_prices.py sol      # fetch SOL only
"""

import json
import os
import subprocess
import sys
import time as _time
from datetime import datetime, timezone, timedelta


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

COINS = {
    "eth": {
        "coingecko_id": "ethereum",
        "output_file": "eth_price.json",
        "description": "ETH/USD daily closing prices",
        "start_date": "2024-07-01",  # ETH ETFs launched Jul 2024
    },
    "sol": {
        "coingecko_id": "solana",
        "output_file": "sol_price.json",
        "description": "SOL/USD daily closing prices",
        "start_date": "2025-10-01",  # SOL ETFs launched Oct 2025
    },
}


def fetch_price_range(coin_id, ts_from, ts_to):
    """Fetch price data from CoinGecko for a time range."""
    url = (
        f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart/range"
        f"?vs_currency=usd&from={ts_from}&to={ts_to}"
    )
    result = subprocess.run(
        ["curl", "-sL", "--max-time", "30",
         "-H", "Accept: application/json",
         "-H", "User-Agent: CryptoETFDashboard/1.0",
         url],
        capture_output=True, text=True, timeout=60,
    )
    if result.returncode != 0:
        return None

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"  Not JSON: {result.stdout[:100]}")
        return None

    if "prices" not in data:
        if "status" in data:
            print(f"  Rate limited: {data.get('status', {}).get('error_message', '')}")
        return None

    return data["prices"]


def fetch_prices(coin_key):
    """Fetch and save prices for a given coin."""
    config = COINS[coin_key]
    price_file = os.path.join(DATA_DIR, config["output_file"])
    coin_id = config["coingecko_id"]

    print(f"\n{'='*50}")
    print(f"Fetching {coin_key.upper()} prices ({coin_id})")
    print(f"{'='*50}")

    # Load existing
    prices = {}
    if os.path.exists(price_file):
        try:
            with open(price_file) as f:
                existing = json.load(f)
            prices = existing.get("prices", {})
            print(f"  Existing: {len(prices)} days")
        except Exception:
            pass

    # Build date ranges (90-day chunks)
    start = datetime.strptime(config["start_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    chunk_days = 90

    ranges = []
    cursor = start
    while cursor < now:
        chunk_end = min(cursor + timedelta(days=chunk_days), now)
        ranges.append((cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cursor = chunk_end

    total_new = 0
    for start_str, end_str in ranges:
        start_dt = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        if start_dt > now:
            continue

        # Skip if good coverage exists
        existing_in_range = sum(1 for d in prices if start_str <= d <= end_str)
        expected_days = (min(end_dt, now) - start_dt).days
        if expected_days > 0 and existing_in_range >= expected_days * 0.9:
            print(f"  [{start_str}~{end_str}] Already have {existing_in_range} days, skipping")
            continue

        ts_from = int(start_dt.timestamp())
        ts_to = int(min(end_dt, now).timestamp())

        raw_prices = fetch_price_range(coin_id, ts_from, ts_to)
        if raw_prices is None:
            print(f"  [{start_str}~{end_str}] Failed")
            continue

        count = 0
        for ts_ms, price in raw_prices:
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            date_iso = dt.strftime("%Y-%m-%d")
            prices[date_iso] = round(price, 2)
            count += 1

        total_new += count
        print(f"  [{start_str}~{end_str}] Got {count} data points")

        # Respect rate limit (free: ~10 calls/min)
        _time.sleep(7)

    # Save
    result_data = {
        "description": config["description"],
        "source": "CoinGecko",
        "last_updated": now.isoformat(),
        "prices": dict(sorted(prices.items())),
    }

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(price_file, "w") as f:
        json.dump(result_data, f, indent=2)

    print(f"[OK] {coin_key.upper()} prices: {total_new} new → {len(prices)} total days")


def main():
    targets = sys.argv[1:] if len(sys.argv) > 1 else list(COINS.keys())

    for t in targets:
        t = t.lower()
        if t not in COINS:
            print(f"[WARN] Unknown coin: {t} (available: {', '.join(COINS.keys())})")
            continue
        try:
            fetch_prices(t)
        except Exception as e:
            print(f"[ERROR] {t.upper()} price fetch failed: {e}")


if __name__ == "__main__":
    main()
