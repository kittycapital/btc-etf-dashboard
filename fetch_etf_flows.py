#!/usr/bin/env python3
"""
Bitcoin ETF Flow Data Fetcher
Scrapes daily ETF flow data from bitbo.io and saves to JSON.
Designed to run via GitHub Actions on a daily schedule.
"""

import json
import os
import re
import sys
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from html.parser import HTMLParser


# ─── Configuration ───────────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
FLOWS_FILE = os.path.join(DATA_DIR, "etf_flows.json")
SOURCE_URL = "https://bitbo.io/treasuries/etf-flows/"

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


# ─── HTML Parser ─────────────────────────────────────────────────────────────
class ETFTableParser(HTMLParser):
    """Parse ETF flow table from bitbo.io HTML."""

    def __init__(self):
        super().__init__()
        self.in_table = False
        self.in_thead = False
        self.in_tbody = False
        self.in_tr = False
        self.in_td = False
        self.in_th = False
        self.current_row = []
        self.headers = []
        self.rows = []
        self.current_data = ""

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table":
            self.in_table = True
        elif tag == "thead" and self.in_table:
            self.in_thead = True
        elif tag == "tbody" and self.in_table:
            self.in_tbody = True
        elif tag == "tr" and self.in_table:
            self.in_tr = True
            self.current_row = []
        elif tag == "th" and self.in_tr:
            self.in_th = True
            self.current_data = ""
        elif tag == "td" and self.in_tr:
            self.in_td = True
            self.current_data = ""

    def handle_endtag(self, tag):
        if tag == "table":
            self.in_table = False
        elif tag == "thead":
            self.in_thead = False
        elif tag == "tbody":
            self.in_tbody = False
        elif tag == "tr" and self.in_tr:
            self.in_tr = False
            if self.in_thead and self.current_row:
                self.headers = self.current_row
            elif self.in_tbody and self.current_row:
                self.rows.append(self.current_row)
        elif tag == "th" and self.in_th:
            self.in_th = False
            self.current_row.append(self.current_data.strip())
        elif tag == "td" and self.in_td:
            self.in_td = False
            self.current_row.append(self.current_data.strip())

    def handle_data(self, data):
        if self.in_th or self.in_td:
            self.current_data += data


# ─── Data Fetching ───────────────────────────────────────────────────────────
def fetch_html(url: str) -> str:
    """Fetch HTML content from URL."""
    req = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; BTCETFDashboard/1.0)"
    })
    with urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8")


def parse_flow_value(val: str) -> float | None:
    """Parse a flow value string like '-277.2' or '0.0' into float."""
    if not val or val == "-":
        return None
    cleaned = val.replace(",", "").replace(" ", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_date(date_str: str) -> str | None:
    """Parse date string like 'Feb 04, 2026' into ISO format 'YYYY-MM-DD'."""
    try:
        dt = datetime.strptime(date_str.strip(), "%b %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def scrape_etf_flows() -> list[dict]:
    """Scrape ETF flow data from bitbo.io."""
    print(f"[INFO] Fetching data from {SOURCE_URL}")
    html = fetch_html(SOURCE_URL)

    parser = ETFTableParser()
    parser.feed(html)

    if not parser.headers or not parser.rows:
        print("[ERROR] Could not find ETF flow table in HTML")
        return []

    print(f"[INFO] Found {len(parser.rows)} rows")

    records = []
    for row in parser.rows:
        if not row or len(row) < 2:
            continue

        date_str = row[0]
        date_iso = parse_date(date_str)

        # Skip summary rows (Total, Average, Maximum, Minimum)
        if not date_iso:
            continue

        flows = {}
        total = 0.0
        for i, ticker in enumerate(ETF_TICKERS):
            col_idx = i + 1
            if col_idx < len(row):
                val = parse_flow_value(row[col_idx])
                flows[ticker] = val if val is not None else 0.0
                total += flows[ticker]
            else:
                flows[ticker] = 0.0

        # Use the "Totals" column if available
        totals_idx = len(ETF_TICKERS) + 1
        if totals_idx < len(row):
            parsed_total = parse_flow_value(row[totals_idx])
            if parsed_total is not None:
                total = parsed_total

        records.append({
            "date": date_iso,
            "flows": flows,
            "total": round(total, 1),
        })

    # Sort by date ascending
    records.sort(key=lambda x: x["date"])
    return records


# ─── Data Storage ────────────────────────────────────────────────────────────
def load_existing_data() -> dict:
    """Load existing data from JSON file."""
    if os.path.exists(FLOWS_FILE):
        with open(FLOWS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "metadata": {
            "description": "Bitcoin Spot ETF Daily Net Flows (US$M)",
            "source": "bitbo.io/treasuries/etf-flows",
            "etf_info": ETF_INFO,
            "tickers": ETF_TICKERS,
            "last_updated": None,
        },
        "daily_flows": [],
    }


def merge_data(existing: dict, new_records: list[dict]) -> dict:
    """Merge new records into existing data, avoiding duplicates."""
    existing_dates = {r["date"] for r in existing["daily_flows"]}

    added = 0
    updated = 0
    for record in new_records:
        if record["date"] in existing_dates:
            # Update existing record
            for i, r in enumerate(existing["daily_flows"]):
                if r["date"] == record["date"]:
                    existing["daily_flows"][i] = record
                    updated += 1
                    break
        else:
            existing["daily_flows"].append(record)
            added += 1

    # Sort by date
    existing["daily_flows"].sort(key=lambda x: x["date"])

    # Update metadata
    existing["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()

    print(f"[INFO] Added {added} new records, updated {updated} existing records")
    print(f"[INFO] Total records: {len(existing['daily_flows'])}")

    return existing


def save_data(data: dict):
    """Save data to JSON file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(FLOWS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"[INFO] Saved to {FLOWS_FILE}")


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    try:
        new_records = scrape_etf_flows()
        if not new_records:
            print("[WARN] No new records scraped")
            sys.exit(1)

        existing = load_existing_data()
        merged = merge_data(existing, new_records)
        save_data(merged)

        print("[OK] ETF flow data updated successfully")

    except Exception as e:
        print(f"[ERROR] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
