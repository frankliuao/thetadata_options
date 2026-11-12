#!/usr/bin/env python3
"""Fetch the latest closing prices for all S&P 500 universe stocks using yfinance."""

import csv
import sys
import datetime
from pathlib import Path

try:
    import yfinance as yf
except ImportError:
    print("Error: 'yfinance' is not installed. Run 'poetry install' or 'pip install yfinance' to use this script.")
    sys.exit(1)

import pandas as pd

def main():
    csv_input = Path("config/sp500_universe.csv")
    if not csv_input.exists():
        print(f"Error: {csv_input} not found.")
        sys.exit(1)

    universe_meta = {}
    tickers = []
    with open(csv_input, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sym = row.get("ticker", "").strip().upper()
            if sym:
                tickers.append(sym)
                universe_meta[sym] = {
                    "company_name": row.get("company_name", ""),
                    "trading_status": row.get("trading_status") or row.get("gics_sector", ""),
                    "sp500_status": row.get("sp500_status", ""),
                }

    print(f"Loaded {len(tickers)} symbols from {csv_input}.")

    # Map tickers with dots to hyphens for Yahoo Finance (e.g. BRK.B -> BRK-B, BF.B -> BF-B)
    yf_symbol_map = {sym: sym.replace(".", "-") for sym in tickers}
    yf_tickers = list(yf_symbol_map.values())

    print(f"Fetching 5-day historical data from yfinance for {len(yf_tickers)} symbols...")
    # Fetch 5 days so we can reliably get the latest close and previous close
    data = yf.download(
        tickers=yf_tickers,
        period="5d",
        interval="1d",
        group_by="column",
        auto_adjust=False,
        threads=True,
        progress=True,
    )

    close_df = data["Close"] if "Close" in data else pd.DataFrame()
    open_df = data["Open"] if "Open" in data else pd.DataFrame()
    high_df = data["High"] if "High" in data else pd.DataFrame()
    low_df = data["Low"] if "Low" in data else pd.DataFrame()
    volume_df = data["Volume"] if "Volume" in data else pd.DataFrame()

    results = []
    failed_symbols = []

    for sym in tickers:
        yf_sym = yf_symbol_map[sym]
        meta = universe_meta[sym]

        row_res = {
            "ticker": sym,
            "yf_ticker": yf_sym,
            "company_name": meta["company_name"],
            "trading_status": meta["trading_status"],
            "sp500_status": meta["sp500_status"],
            "date": "",
            "close": None,
            "change": None,
            "pct_change": None,
            "open": None,
            "high": None,
            "low": None,
            "volume": None,
            "prev_close": None,
        }

        if yf_sym in close_df.columns:
            s_close = close_df[yf_sym].dropna()
            if len(s_close) > 0:
                latest_date = s_close.index[-1]
                latest_close = float(s_close.iloc[-1])
                row_res["date"] = latest_date.strftime("%Y-%m-%d")
                row_res["close"] = round(latest_close, 2)

                # Previous close & change
                if len(s_close) >= 2:
                    prev_close = float(s_close.iloc[-2])
                    change = latest_close - prev_close
                    pct_change = (change / prev_close) * 100.0
                    row_res["prev_close"] = round(prev_close, 2)
                    row_res["change"] = round(change, 2)
                    row_res["pct_change"] = round(pct_change, 2)

                if yf_sym in open_df.columns:
                    s_open = open_df[yf_sym].dropna()
                    if len(s_open) > 0:
                        row_res["open"] = round(float(s_open.iloc[-1]), 2)
                if yf_sym in high_df.columns:
                    s_high = high_df[yf_sym].dropna()
                    if len(s_high) > 0:
                        row_res["high"] = round(float(s_high.iloc[-1]), 2)
                if yf_sym in low_df.columns:
                    s_low = low_df[yf_sym].dropna()
                    if len(s_low) > 0:
                        row_res["low"] = round(float(s_low.iloc[-1]), 2)
                if yf_sym in volume_df.columns:
                    s_vol = volume_df[yf_sym].dropna()
                    if len(s_vol) > 0:
                        row_res["volume"] = int(s_vol.iloc[-1])

                results.append(row_res)
                continue

        failed_symbols.append(sym)
        results.append(row_res)

    # Sort results by ticker
    results.sort(key=lambda x: x["ticker"])

    output_csv = Path("sp500_latest_prices.csv")
    fieldnames = [
        "ticker",
        "company_name",
        "trading_status",
        "sp500_status",
        "date",
        "close",
        "change",
        "pct_change",
        "open",
        "high",
        "low",
        "volume",
        "prev_close",
    ]

    with open(output_csv, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    success_count = len(results) - len(failed_symbols)
    print(f"\nSuccessfully fetched {success_count} / {len(tickers)} stocks.")
    if failed_symbols:
        print(f"Failed/Missing symbols ({len(failed_symbols)}): {failed_symbols[:15]} ...")
    print(f"Results written to: {output_csv.resolve()}")

if __name__ == "__main__":
    main()
