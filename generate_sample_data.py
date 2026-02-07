#!/usr/bin/env python3
"""Generate sample ETF flow data for dashboard demonstration."""

import json
import os
import random
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
FLOWS_FILE = os.path.join(DATA_DIR, "etf_flows.json")

ETF_TICKERS = ["IBIT", "FBTC", "GBTC", "BTC", "BITB", "ARKB", "HODL", "BTCO", "BRRR", "EZBC", "BTCW", "DEFI"]

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

# Realistic flow patterns (IBIT dominates, GBTC often negative, small ETFs near zero)
def generate_day_flows(trend_bias=0):
    """Generate realistic daily flows for all ETFs."""
    flows = {}
    
    # IBIT: -500 to +800, biggest player
    flows["IBIT"] = round(random.gauss(50 + trend_bias * 100, 200), 1)
    flows["IBIT"] = max(-600, min(800, flows["IBIT"]))
    
    # FBTC: -200 to +400
    flows["FBTC"] = round(random.gauss(20 + trend_bias * 50, 100), 1)
    flows["FBTC"] = max(-300, min(400, flows["FBTC"]))
    
    # GBTC: mostly negative outflows
    flows["GBTC"] = round(random.gauss(-30, 50), 1)
    flows["GBTC"] = max(-200, min(50, flows["GBTC"]))
    
    # BTC (mini trust): small positive
    flows["BTC"] = round(random.gauss(5, 15), 1)
    flows["BTC"] = max(-30, min(50, flows["BTC"]))
    
    # BITB
    flows["BITB"] = round(random.gauss(10 + trend_bias * 20, 40), 1)
    flows["BITB"] = max(-100, min(150, flows["BITB"]))
    
    # ARKB
    flows["ARKB"] = round(random.gauss(10 + trend_bias * 20, 50), 1)
    flows["ARKB"] = max(-100, min(200, flows["ARKB"]))
    
    # Small ETFs
    for ticker in ["HODL", "BTCO", "BRRR", "EZBC", "BTCW", "DEFI"]:
        flows[ticker] = round(random.gauss(0, 10), 1)
        flows[ticker] = max(-30, min(30, flows[ticker]))
    
    total = round(sum(flows.values()), 1)
    return flows, total


def generate_sample_data():
    """Generate ~90 days of sample ETF flow data."""
    random.seed(42)
    
    start_date = datetime(2025, 11, 1)
    end_date = datetime(2026, 2, 4)
    
    records = []
    current = start_date
    
    # Simulate different market phases
    while current <= end_date:
        # Skip weekends
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue
        
        # Market phases
        day_num = (current - start_date).days
        if day_num < 30:
            trend = 0.3  # Bullish Nov
        elif day_num < 50:
            trend = 0.5  # Strong Dec rally
        elif day_num < 65:
            trend = -0.2  # Year-end profit taking
        elif day_num < 80:
            trend = -0.5  # Jan correction
        else:
            trend = -0.3  # Feb weakness
        
        flows, total = generate_day_flows(trend)
        
        records.append({
            "date": current.strftime("%Y-%m-%d"),
            "flows": flows,
            "total": total,
        })
        
        current += timedelta(days=1)
    
    data = {
        "metadata": {
            "description": "Bitcoin Spot ETF Daily Net Flows (US$M)",
            "source": "bitbo.io/treasuries/etf-flows",
            "etf_info": ETF_INFO,
            "tickers": ETF_TICKERS,
            "last_updated": "2026-02-04T22:00:00+00:00",
        },
        "daily_flows": records,
    }
    
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(FLOWS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"Generated {len(records)} days of sample data → {FLOWS_FILE}")


if __name__ == "__main__":
    generate_sample_data()
