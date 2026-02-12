#!/usr/bin/env python3
"""
Farside Investors ETF Flow Scraper
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Scrapes ETH & SOL ETF flow tables from farside.co.uk
No API key required. Designed for GitHub Actions.

Data source: https://farside.co.uk/eth/ , https://farside.co.uk/sol/
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

# Asset configs — easy to add more later (e.g. BTC from Farside)
ASSETS = {
    "eth": {
        "url": "https://farside.co.uk/eth/",
        "output_file": "eth_etf_flows.json",
        "description": "Ethereum Spot ETF Daily Net Flows (US$M)",
        "etf_info": {
            "ETHA": {"name": "iShares Ethereum Trust", "issuer": "BlackRock"},
            "FETH": {"name": "Fidelity Ethereum Fund", "issuer": "Fidelity"},
            "ETHW": {"name": "Bitwise Ethereum ETF", "issuer": "Bitwise"},
            "TETH": {"name": "21Shares Core Ethereum ETF", "issuer": "21Shares"},
            "ETHV": {"name": "VanEck Ethereum ETF", "issuer": "VanEck"},
            "QETH": {"name": "Invesco Galaxy Ethereum ETF", "issuer": "Invesco"},
            "EZET": {"name": "Franklin Ethereum ETF", "issuer": "Franklin Templeton"},
            "ETHE": {"name": "Grayscale Ethereum Trust", "issuer": "Grayscale"},
            "ETH":  {"name": "Grayscale Ethereum Mini Trust", "issuer": "Grayscale"},
        },
    },
    "sol": {
        "url": "https://farside.co.uk/sol/",
        "output_file": "sol_etf_flows.json",
        "description": "Solana Spot ETF Daily Net Flows (US$M)",
        "etf_info": {
            "BSOL": {"name": "Bitwise Solana Staking ETF", "issuer": "Bitwise"},
            "VSOL": {"name": "VanEck Solana ETF", "issuer": "VanEck"},
            "FSOL": {"name": "Fidelity Solana Fund", "issuer": "Fidelity"},
            "TSOL": {"name": "21Shares Core Solana ETF", "issuer": "21Shares"},
            "SOEZ": {"name": "Franklin Solana ETF", "issuer": "Franklin Templeton"},
            "GSOL": {"name": "Grayscale Solana Trust", "issuer": "Grayscale"},
        },
    },
}

MONTHS = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
    "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
    "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


# ─── Fetch HTML — multi-strategy (Cloudflare bypass) ────────────────────────

def fetch_html(url: str) -> str:
    """
    Fetch page with multiple strategies to bypass Cloudflare.
    Order: cloudscraper → playwright → curl fallback
    """
    print(f"[INFO] Fetching {url}")

    # Strategy 1: cloudscraper (handles basic Cloudflare UAM)
    html = _fetch_cloudscraper(url)
    if html and _is_valid_html(html):
        print(f"[INFO] cloudscraper OK — {len(html):,} bytes")
        return html

    # Strategy 2: playwright (headless browser, handles JS challenges)
    html = _fetch_playwright(url)
    if html and _is_valid_html(html):
        print(f"[INFO] playwright OK — {len(html):,} bytes")
        return html

    # Strategy 3: curl with enhanced headers (last resort)
    html = _fetch_curl(url)
    if html and _is_valid_html(html):
        print(f"[INFO] curl OK — {len(html):,} bytes")
        return html

    raise RuntimeError(
        f"All fetch strategies failed for {url}. "
        "Farside may be blocking this IP range."
    )


def _is_valid_html(html: str) -> bool:
    """Check if we got real page content, not a Cloudflare challenge."""
    if len(html) < 5000:
        print(f"  [SKIP] Too short ({len(html)} bytes)")
        return False
    if "class=\"etf\"" not in html and "class='etf'" not in html:
        # Also check for table tag as loose match
        if "<table" not in html:
            print(f"  [SKIP] No ETF table found in {len(html)} bytes")
            return False
    return True


def _fetch_cloudscraper(url: str) -> str | None:
    """Strategy 1: cloudscraper library."""
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'linux',
                'desktop': True,
            }
        )
        resp = scraper.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text
    except ImportError:
        print("  [cloudscraper] Not installed, skipping")
        return None
    except Exception as e:
        print(f"  [cloudscraper] Failed: {e}")
        return None


def _fetch_playwright(url: str) -> str | None:
    """Strategy 2: Playwright headless Chromium."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  [playwright] Not installed, skipping")
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()
            page.goto(url, wait_until="networkidle", timeout=30000)

            # Wait for the ETF table to appear
            try:
                page.wait_for_selector("table.etf", timeout=10000)
            except Exception:
                pass  # Still grab whatever we got

            html = page.content()
            browser.close()
            return html
    except Exception as e:
        print(f"  [playwright] Failed: {e}")
        return None


def _fetch_curl(url: str) -> str | None:
    """Strategy 3: curl with comprehensive browser headers."""
    try:
        result = subprocess.run(
            [
                "curl", "-sL",
                "--max-time", "30",
                "-H", "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "-H", "Accept-Language: en-US,en;q=0.9",
                "-H", "Accept-Encoding: identity",
                "-H", "Cache-Control: no-cache",
                "-H", "Pragma: no-cache",
                "-H", "Sec-Fetch-Dest: document",
                "-H", "Sec-Fetch-Mode: navigate",
                "-H", "Sec-Fetch-Site: none",
                "-H", "Sec-Fetch-User: ?1",
                "-H", "Upgrade-Insecure-Requests: 1",
                url,
            ],
            capture_output=True, text=True, timeout=60,
        )

        if result.returncode != 0:
            print(f"  [curl] Exit code {result.returncode}")
            return None

        return result.stdout
    except Exception as e:
        print(f"  [curl] Failed: {e}")
        return None


# ─── Parsing helpers ─────────────────────────────────────────────────────────

# Farside date format: "23 Jan 2026" (DD Mon YYYY)
DATE_RE = re.compile(
    r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})"
)


def parse_date(text: str) -> str | None:
    """'23 Jan 2026' → '2026-01-23' or None."""
    m = DATE_RE.search(text)
    if not m:
        return None
    day, mon, year = m.group(1), m.group(2), m.group(3)
    return f"{year}-{MONTHS[mon]}-{int(day):02d}"


def parse_farside_num(text: str) -> float | None:
    """
    Parse Farside number formats:
      '44.5'    → 44.5
      '(44.5)'  → -44.5    (parentheses = negative)
      '-'       → None     (pending/no data)
      '0.0'     → 0.0
      '9,199*'  → 9199.0   (seed values with asterisk)
    """
    s = text.strip().replace(",", "").replace("\xa0", "").replace("*", "")

    if not s or s == "-" or s == "—":
        return None  # Pending data

    # Parenthesized negative: (44.5) → -44.5
    paren = re.match(r"^\(([0-9.]+)\)$", s)
    if paren:
        try:
            return round(-float(paren.group(1)), 1)
        except ValueError:
            return None

    try:
        return round(float(s), 1)
    except ValueError:
        return None


def strip_html(html_fragment: str) -> str:
    """Remove all HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", html_fragment).strip()


# ─── Table parser ────────────────────────────────────────────────────────────
def parse_farside_table(html: str, asset_key: str) -> tuple[list[str], list[dict]]:
    """
    Parse Farside ETF flow table.

    Returns:
        (tickers, records)
        tickers: list of ticker symbols found in header
        records: list of {date, flows: {ticker: value}, total}
    """
    # Find table with class="etf" (flexible matching)
    table_match = re.search(
        r'<table[^>]*class=["\']etf["\'][^>]*>(.*?)</table>',
        html, re.DOTALL | re.I
    )
    if not table_match:
        # Fallback: any table with "etf" in class
        table_match = re.search(
            r'<table[^>]*class=["\'][^"\']*etf[^"\']*["\'][^>]*>(.*?)</table>',
            html, re.DOTALL | re.I
        )
    if not table_match:
        # Log what we DO see for debugging
        tables = re.findall(r'<table[^>]*>', html[:5000], re.I)
        print(f"[ERROR] No table.etf found for {asset_key}")
        print(f"[DEBUG] Tables in first 5KB: {tables[:5]}")
        print(f"[DEBUG] HTML starts with: {html[:300]}")
        return [], []

    table_html = table_match.group(1)

    # ── Extract tickers from <thead> ──
    # Ticker row is the 2nd row in thead (index 1)
    thead_match = re.search(r"<thead>(.*?)</thead>", table_html, re.DOTALL | re.I)
    if not thead_match:
        print(f"[ERROR] No thead found for {asset_key}")
        return [], []

    thead_rows = re.findall(r"<tr[^>]*>(.*?)</tr>", thead_match.group(1), re.DOTALL | re.I)
    if len(thead_rows) < 2:
        print(f"[ERROR] Expected ≥2 thead rows, got {len(thead_rows)}")
        return [], []

    # Parse ticker row (2nd row, index 1)
    ticker_cells = [
        strip_html(c) for c in
        re.findall(r"<th[^>]*>(.*?)</th>", thead_rows[1], re.DOTALL | re.I)
    ]
    # First cell is empty (date column), last may be empty (Total column)
    # Filter to only actual ticker symbols
    tickers = [t for t in ticker_cells if t and t != "Total" and re.match(r'^[A-Z]{2,5}$', t)]
    print(f"[INFO] Found tickers for {asset_key}: {tickers}")

    if not tickers:
        print(f"[ERROR] No tickers found in header for {asset_key}")
        return [], []

    # ── Extract data rows from <tbody> ──
    tbody_match = re.search(r"<tbody>(.*?)</tbody>", table_html, re.DOTALL | re.I)
    if not tbody_match:
        print(f"[ERROR] No tbody found for {asset_key}")
        return tickers, []

    data_rows = re.findall(r"<tr[^>]*>(.*?)</tr>", tbody_match.group(1), re.DOTALL | re.I)

    # Labels to skip (summary/header rows)
    SKIP_LABELS = {"Seed", "Total", "Average", "Maximum", "Minimum"}

    records = []
    for row_html in data_rows:
        cells = [
            strip_html(c) for c in
            re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.DOTALL | re.I)
        ]

        if not cells:
            continue

        # Skip non-data rows
        first_cell = cells[0].strip()
        if first_cell in SKIP_LABELS:
            continue

        # Parse date
        date = parse_date(first_cell)
        if not date:
            continue

        # Parse flow values (cells 1..N map to tickers)
        flows = {}
        all_pending = True
        for i, ticker in enumerate(tickers):
            col_idx = i + 1
            if col_idx < len(cells):
                val = parse_farside_num(cells[col_idx])
                flows[ticker] = val if val is not None else 0.0
                if val is not None:
                    all_pending = False
            else:
                flows[ticker] = 0.0

        # Skip rows where all individual tickers are pending ("-")
        # These are current-day rows with no data yet
        if all_pending:
            continue

        # Total is the last column
        total_idx = len(tickers) + 1
        if total_idx < len(cells):
            total_val = parse_farside_num(cells[total_idx])
            total = total_val if total_val is not None else round(sum(flows.values()), 1)
        else:
            total = round(sum(flows.values()), 1)

        records.append({
            "date": date,
            "flows": flows,
            "total": total,
        })

    return tickers, records


# ─── Data persistence ────────────────────────────────────────────────────────
def load_existing(filepath: str, asset_config: dict, tickers: list[str]) -> dict:
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "metadata": {
            "description": asset_config["description"],
            "source": "farside.co.uk",
            "etf_info": asset_config["etf_info"],
            "tickers": tickers,
            "last_updated": None,
        },
        "daily_flows": [],
    }


def merge_and_save(existing: dict, new_records: list[dict], filepath: str, tickers: list[str]) -> None:
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
    existing["metadata"]["tickers"] = tickers

    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)

    total = len(existing["daily_flows"])
    print(f"[INFO] +{added} new, ~{updated} updated → {total} total records")
    print(f"[OK] Saved → {filepath}")


# ─── Process a single asset ──────────────────────────────────────────────────
def process_asset(asset_key: str) -> bool:
    """Fetch and parse one asset (eth/sol). Returns True on success."""
    config = ASSETS[asset_key]
    output_path = os.path.join(DATA_DIR, config["output_file"])

    print(f"\n{'='*60}")
    print(f"  Processing: {asset_key.upper()} ETF Flows")
    print(f"{'='*60}")

    try:
        html = fetch_html(config["url"])
    except Exception as e:
        print(f"[ERROR] Failed to fetch {asset_key}: {e}")
        return False

    tickers, records = parse_farside_table(html, asset_key)

    if not records:
        print(f"[ERROR] No records parsed for {asset_key}")

        # Save debug HTML
        os.makedirs(DATA_DIR, exist_ok=True)
        debug_path = os.path.join(DATA_DIR, f"_debug_{asset_key}.html")
        with open(debug_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[DEBUG] Saved raw HTML → {debug_path}")
        return False

    records.sort(key=lambda x: x["date"])
    print(f"[INFO] Parsed {len(records)} records: {records[0]['date']} → {records[-1]['date']}")
    print(f"[INFO] Latest: {records[-1]['date']}  total={records[-1]['total']}M")

    existing = load_existing(output_path, config, tickers)
    merge_and_save(existing, records, output_path, tickers)
    return True


# ─── Entry point ─────────────────────────────────────────────────────────────
def main():
    import time as _time

    # Parse CLI args: specific assets or all
    if len(sys.argv) > 1:
        targets = [a.lower() for a in sys.argv[1:] if a.lower() in ASSETS]
    else:
        targets = list(ASSETS.keys())  # Default: all

    if not targets:
        print(f"Usage: {sys.argv[0]} [eth] [sol]")
        print(f"Available assets: {', '.join(ASSETS.keys())}")
        sys.exit(1)

    results = {}
    for i, asset_key in enumerate(targets):
        results[asset_key] = process_asset(asset_key)

        # Delay between requests to be polite
        if i < len(targets) - 1:
            print("[INFO] Waiting 3s before next request...")
            _time.sleep(3)

    # Summary
    print(f"\n{'='*60}")
    print("  Summary")
    print(f"{'='*60}")
    for asset, ok in results.items():
        status = "✓ OK" if ok else "✗ FAILED"
        print(f"  {asset.upper()}: {status}")

    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
