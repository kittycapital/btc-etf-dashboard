#!/usr/bin/env python3
"""
Bitcoin ETF Flow Data Fetcher
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Scrapes ETF flow table from bitbo.io using curl.
No API key required. Designed for GitHub Actions.
"""

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone


# ─── Configuration ───────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
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

MONTHS = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


# ─── Fetch HTML via curl ─────────────────────────────────────────────────────
def fetch_html() -> str:
    """Fetch the page using curl with realistic browser headers."""
    print(f"[INFO] Fetching data from {SOURCE_URL}")

    result = subprocess.run(
        [
            "curl", "-sL",
            "--max-time", "30",
            "-H", "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "-H", "Accept-Language: en-US,en;q=0.9",
            SOURCE_URL,
        ],
        capture_output=True, text=True, timeout=60,
    )

    if result.returncode != 0:
        raise RuntimeError(f"curl failed (exit {result.returncode}): {result.stderr[:300]}")

    html = result.stdout
    print(f"[INFO] Received {len(html):,} bytes")
    return html


# ─── Date parsing ────────────────────────────────────────────────────────────
DATE_RE = re.compile(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s*(\d{4})")

def parse_date(text: str) -> str | None:
    """'Feb 04, 2026' → '2026-02-04'  or None."""
    m = DATE_RE.search(text)
    if not m:
        return None
    mon, day, year = m.group(1), m.group(2), m.group(3)
    return f"{year}-{MONTHS[mon]}-{int(day):02d}"


def parse_num(text: str) -> float:
    """Parse a number string like '-277.2' or '-0.0' → float."""
    s = text.strip().replace(",", "").replace("\xa0", "")
    if not s or s == "-" or s == "—":
        return 0.0
    try:
        return round(float(s), 1)
    except ValueError:
        return 0.0


# ─── Parsing strategies ─────────────────────────────────────────────────────
def parse_html_table(html: str) -> list[dict]:
    """Strategy 1: Parse <table> with <tr>/<td> tags."""
    # Find table containing ETF tickers
    tables = re.findall(r"<table[^>]*>(.*?)</table>", html, re.DOTALL | re.I)
    for table_html in tables:
        if "IBIT" not in table_html:
            continue

        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL | re.I)
        records = []
        for row_html in rows:
            cells = [
                re.sub(r"<[^>]+>", "", c).strip()
                for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.DOTALL | re.I)
            ]
            if len(cells) < 13:
                continue
            date = parse_date(cells[0])
            if not date:
                continue

            flows = {}
            for i, ticker in enumerate(ETF_TICKERS):
                flows[ticker] = parse_num(cells[i + 1]) if (i + 1) < len(cells) else 0.0

            total_col = len(ETF_TICKERS) + 1
            total = parse_num(cells[total_col]) if total_col < len(cells) else round(sum(flows.values()), 1)

            records.append({"date": date, "flows": flows, "total": total})

        if records:
            return records

    return []


def parse_pipe_table(html: str) -> list[dict]:
    """Strategy 2: Parse markdown-style pipe-separated table."""
    records = []
    for line in html.split("\n"):
        line = line.strip()
        if "|" not in line:
            continue
        # Skip separator rows like |---|---|
        if re.match(r"^\|[\s\-|]+\|$", line):
            continue

        cells = [c.strip() for c in line.split("|")]
        # Remove empty first/last from leading/trailing pipes
        cells = [c for c in cells if c != ""]

        if len(cells) < 13:
            continue

        date = parse_date(cells[0])
        if not date:
            continue

        flows = {}
        for i, ticker in enumerate(ETF_TICKERS):
            flows[ticker] = parse_num(cells[i + 1]) if (i + 1) < len(cells) else 0.0

        total_col = len(ETF_TICKERS) + 1
        total = parse_num(cells[total_col]) if total_col < len(cells) else round(sum(flows.values()), 1)

        records.append({"date": date, "flows": flows, "total": total})

    return records


def parse_json_data(html: str) -> list[dict]:
    """Strategy 3: Look for JSON data in <script> or data attributes."""
    # Look for __NEXT_DATA__ or similar embedded JSON
    patterns = [
        r"__NEXT_DATA__[^{]*({.*?})\s*</script>",
        r'type="application/json"[^>]*>(.*?)</script>',
        r'"dailyFlows"\s*:\s*(\[.*?\])',
        r'"etf_flows"\s*:\s*(\[.*?\])',
    ]
    for pat in patterns:
        m = re.search(pat, html, re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(1))
                print(f"[DEBUG] Found embedded JSON ({type(data).__name__})")
                # Would need site-specific parsing here
            except json.JSONDecodeError:
                continue
    return []


# ─── Main extraction ─────────────────────────────────────────────────────────
def extract_records(html: str) -> list[dict]:
    """Try all parsing strategies and return the first that works."""

    # Strategy 1: HTML <table>
    records = parse_html_table(html)
    if records:
        print(f"[OK] HTML table parser found {len(records)} records")
        return records

    # Strategy 2: Pipe-separated (markdown) table
    records = parse_pipe_table(html)
    if records:
        print(f"[OK] Pipe table parser found {len(records)} records")
        return records

    # Strategy 3: Embedded JSON
    records = parse_json_data(html)
    if records:
        print(f"[OK] JSON parser found {len(records)} records")
        return records

    # All strategies failed — dump debug info
    print("[ERROR] Could not parse ETF data from HTML")
    for tag in ["<table", "IBIT", "|", "etf-flow", "Feb", "Jan"]:
        count = html.lower().count(tag.lower())
        idx = html.find(tag)
        snippet = html[max(0, idx - 30):idx + 80] if idx >= 0 else "(not found)"
        print(f"  '{tag}' → {count} occurrences, first at {idx}: {repr(snippet[:100])}")

    # Save HTML for manual debugging
    os.makedirs(DATA_DIR, exist_ok=True)
    debug_path = os.path.join(DATA_DIR, "_debug_response.html")
    with open(debug_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[DEBUG] Saved raw HTML → {debug_path}")

    return []


# ─── Data persistence ────────────────────────────────────────────────────────
def load_existing() -> dict:
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


def merge_and_save(existing: dict, new_records: list[dict]) -> None:
    date_idx = {r["date"]: i for i, r in enumerate(existing["daily_flows"])}
    added = updated = 0

    for rec in new_records:
        if rec["date"] in date_idx:
            existing["daily_flows"][date_idx[rec["date"]]] = rec
            updated += 1
        else:
            existing["daily_flows"].append(rec)
            added += 1

    existing["daily_flows"].sort(key=lambda x: x["date"])
    existing["metadata"]["last_updated"] = datetime.now(timezone.utc).isoformat()

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(FLOWS_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

    total = len(existing["daily_flows"])
    print(f"[INFO] +{added} new, ~{updated} updated → {total} total records")
    print(f"[OK] Saved → {FLOWS_FILE}")


# ─── Entry point ─────────────────────────────────────────────────────────────
def main():
    try:
        html = fetch_html()
        records = extract_records(html)

        if not records:
            print("[FATAL] No ETF flow records extracted")
            sys.exit(1)

        records.sort(key=lambda x: x["date"])
        print(f"[INFO] Date range: {records[0]['date']} → {records[-1]['date']}")
        print(f"[INFO] Latest: {records[-1]['date']}  total={records[-1]['total']}M")

        existing = load_existing()
        merge_and_save(existing, records)

    except Exception as exc:
        print(f"[FATAL] {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
