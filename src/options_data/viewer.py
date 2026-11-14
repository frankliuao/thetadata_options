"""Interactive local web application for visualizing historical Options Prices, IV, and Greeks.

Supports hybrid storage:
1. Instant Polars Predicate Pushdown on local Parquet files (`data/processed/options_eod/`).
2. On-demand lazy pulling of individual symbol-year Parquet files (~5-15 MB) from Google Drive
   so users do not need to keep all 51 GB locally on disk.
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import math
import os
import sys
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import polars as pl
from dotenv import load_dotenv
from rich.console import Console

from options_data.gdrive_sync import DEFAULT_GDRIVE_FOLDER_ID
from options_data.loader import OptionsDataLoader
from options_data.schema import validate_symbol
from options_data.universe import DEFAULT_UNIVERSE_CSV

console = Console()
logger = logging.getLogger(__name__)


def _clean_json_val(v: Any) -> Any:
    """Convert Polars/Python values (dates, datetimes, NaNs, infinities) into JSON-safe primitives."""
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return round(v, 6)
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return v


def _df_to_records(df: pl.DataFrame) -> list[dict[str, Any]]:
    """Convert a Polars DataFrame to a list of JSON-safe dictionaries."""
    rows = df.to_dicts()
    return [{k: _clean_json_val(val) for k, val in row.items()} for row in rows]


class OptionsViewerBackend:
    """Data query engine backing the local Options & Greeks Visualization Web App."""

    def __init__(
        self,
        data_dir: str | Path = "data",
        universe_csv: str | Path = DEFAULT_UNIVERSE_CSV,
        *,
        auto_fetch_gdrive: bool = True,
        gdrive_folder_id: str | None = None,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.universe_csv = Path(universe_csv)
        self.loader = OptionsDataLoader(
            data_dir=self.data_dir,
            auto_fetch_gdrive=auto_fetch_gdrive,
            gdrive_folder_id=gdrive_folder_id,
        )

    def list_symbols(self) -> list[dict[str, Any]]:
        """Return all universe and locally available symbols with local cache statistics."""
        processed_root = self.data_dir / "processed" / "options_eod"
        local_info: dict[str, list[int]] = {}
        if processed_root.exists():
            for sym_dir in processed_root.glob("symbol=*"):
                sym = sym_dir.name.replace("symbol=", "")
                years: list[int] = []
                for pf in sym_dir.glob("*.parquet"):
                    suffix = pf.stem.split("_")[-1]
                    if suffix.isdigit():
                        years.append(int(suffix))
                if years:
                    local_info[sym] = sorted(years)

        records_map: dict[str, dict[str, Any]] = {}
        if self.universe_csv.exists():
            try:
                udf = pl.read_csv(self.universe_csv, ignore_errors=True)
                for rec in udf.to_dicts():
                    sym = str(rec.get("ticker", "")).strip().upper()
                    if not sym:
                        continue
                    years = local_info.get(sym, [])
                    records_map[sym] = {
                        "symbol": sym,
                        "company_name": str(rec.get("company_name") or sym),
                        "cached_years": years,
                        "cached_count": len(years),
                    }
            except Exception as exc:
                logger.warning("Failed to read universe CSV: %s", exc)

        for sym, years in sorted(local_info.items()):
            if sym not in records_map:
                records_map[sym] = {
                    "symbol": sym,
                    "company_name": sym,
                    "cached_years": years,
                    "cached_count": len(years),
                }

        return sorted(records_map.values(), key=lambda x: x["symbol"])

    def get_symbol_metadata(self, symbol: str, year: int | None = None) -> dict[str, Any]:
        """Return available years, expirations, trade dates, and summary stats for `symbol`."""
        sym = validate_symbol(symbol)
        years_filter = [year] if year is not None else None
        try:
            lf = self.loader.scan_symbol(sym, years=years_filter)
        except FileNotFoundError:
            lf = self.loader.scan_symbol(sym)

        all_local_files = self.loader.ensure_local_parquets(sym, years=years_filter)
        available_years = sorted(
            {
                int(p.stem.split("_")[-1])
                for p in (self.data_dir / "processed" / "options_eod" / f"symbol={sym}").glob("*.parquet")
                if p.stem.split("_")[-1].isdigit()
            }
        ) if (self.data_dir / "processed" / "options_eod" / f"symbol={sym}").exists() else []

        exp_df = (
            lf.select(
                [
                    pl.col("expiration").unique().sort(),
                ]
            )
            .collect()
        )
        expirations = [d.isoformat() for d in exp_df["expiration"].drop_nulls().to_list()]

        dates_df = (
            lf.select(
                [
                    pl.col("trade_date").unique().sort(),
                ]
            )
            .collect()
        )
        trade_dates = [d.isoformat() for d in dates_df["trade_date"].drop_nulls().to_list()]

        return {
            "symbol": sym,
            "available_years": available_years,
            "selected_year": year,
            "expirations": expirations,
            "trade_dates": trade_dates,
            "local_files_count": len(all_local_files),
        }

    def get_contracts_for_expiration(
        self,
        symbol: str,
        expiration: str,
        year: int | None = None,
    ) -> dict[str, Any]:
        """Return available strikes and contract metadata for `(symbol, expiration)`."""
        sym = validate_symbol(symbol)
        exp_date = datetime.date.fromisoformat(expiration)
        years_filter = [year] if year is not None else None
        try:
            lf = self.loader.scan_symbol(sym, years=years_filter).filter(pl.col("expiration") == exp_date)
        except FileNotFoundError:
            lf = self.loader.scan_symbol(sym).filter(pl.col("expiration") == exp_date)

        agg_df = (
            lf.group_by(["strike", "right"])
            .agg(
                [
                    pl.len().alias("days_count"),
                    pl.col("trade_date").min().alias("first_date"),
                    pl.col("trade_date").max().alias("last_date"),
                    pl.col("open_interest").max().alias("max_oi"),
                    pl.col("volume").sum().alias("total_volume"),
                    pl.col("underlying_price").last().alias("last_underlying"),
                    pl.col("delta").last().alias("last_delta"),
                ]
            )
            .sort(["strike", "right"])
            .collect()
        )

        contracts = _df_to_records(agg_df)
        strikes = sorted({float(r["strike"]) for r in contracts if r.get("strike") is not None})
        underlying_ref = None
        for r in contracts:
            if r.get("last_underlying") is not None and r["last_underlying"] > 0:
                underlying_ref = r["last_underlying"]
                break

        atm_strike = None
        if strikes and underlying_ref:
            atm_strike = min(strikes, key=lambda s: abs(s - underlying_ref))
        elif strikes:
            atm_strike = strikes[len(strikes) // 2]

        return {
            "symbol": sym,
            "expiration": expiration,
            "strikes": strikes,
            "atm_strike": atm_strike,
            "underlying_ref": underlying_ref,
            "contracts": contracts,
        }

    def get_contract_timeseries(
        self,
        symbol: str,
        expiration: str,
        strike: float,
        right: str = "call",
        year: int | None = None,
    ) -> dict[str, Any]:
        """Return full daily price, IV, Open Interest, and Greeks time series for a specific option contract."""
        sym = validate_symbol(symbol)
        exp_date = datetime.date.fromisoformat(expiration)
        right_clean = right.strip().lower()
        years_filter = [year] if year is not None else None
        try:
            lf_base = self.loader.scan_symbol(sym, years=years_filter)
        except FileNotFoundError:
            lf_base = self.loader.scan_symbol(sym)

        lf = (
            lf_base
            .filter(
                (pl.col("expiration") == exp_date)
                & ((pl.col("strike") - strike).abs() < 1e-4)
                & (pl.col("right") == right_clean)
            )
            .sort("trade_date")
        )

        df = lf.collect()
        return {
            "symbol": sym,
            "expiration": expiration,
            "strike": strike,
            "right": right_clean,
            "count": df.height,
            "series": _df_to_records(df),
        }

    def get_chain_smile(
        self,
        symbol: str,
        trade_date: str,
        expiration: str | None = None,
    ) -> dict[str, Any]:
        """Return cross-sectional option chain on `trade_date` for IV Smile & Greeks-by-Strike visualization."""
        sym = validate_symbol(symbol)
        td = datetime.date.fromisoformat(trade_date)
        lf = self.loader.scan_symbol(sym, years=[td.year]).filter(pl.col("trade_date") == td)

        avail_exp_df = lf.select(pl.col("expiration").unique().sort()).collect()
        avail_exps = [d.isoformat() for d in avail_exp_df["expiration"].drop_nulls().to_list()]

        selected_exp = expiration
        if not selected_exp or selected_exp not in avail_exps:
            selected_exp = avail_exps[0] if avail_exps else None

        if selected_exp:
            exp_d = datetime.date.fromisoformat(selected_exp)
            lf = lf.filter(pl.col("expiration") == exp_d)

        df = lf.sort(["strike", "right"]).collect()
        underlying_price = None
        if not df.is_empty() and "underlying_price" in df.columns:
            valid_u = df["underlying_price"].drop_nulls()
            if not valid_u.is_empty():
                underlying_price = _clean_json_val(float(valid_u[0]))

        return {
            "symbol": sym,
            "trade_date": trade_date,
            "available_expirations": avail_exps,
            "selected_expiration": selected_exp,
            "underlying_price": underlying_price,
            "chain": _df_to_records(df),
        }

    def get_option_chain(
        self,
        symbol: str,
        trade_date: str | None = None,
        expiration: str | None = None,
    ) -> dict[str, Any]:
        """Return cross-sectional option chain on `trade_date` with all available expirations and paired strikes."""
        sym = validate_symbol(symbol)

        all_metadata = self.get_symbol_metadata(sym)
        available_dates = all_metadata.get("trade_dates", [])
        if not available_dates:
            return {
                "symbol": sym,
                "trade_date": None,
                "available_trade_dates": [],
                "expirations": [],
                "selected_expiration": None,
                "selected_dte": None,
                "underlying_price": None,
                "strikes": [],
                "totals": {},
            }

        td_str = trade_date if trade_date and trade_date in available_dates else available_dates[-1]
        td_date = datetime.date.fromisoformat(td_str)

        lf = self.loader.scan_symbol(sym, years=[td_date.year]).filter(pl.col("trade_date") == td_date)
        df = lf.collect()
        if df.is_empty():
            return {
                "symbol": sym,
                "trade_date": td_str,
                "available_trade_dates": available_dates,
                "expirations": [],
                "selected_expiration": None,
                "selected_dte": None,
                "underlying_price": None,
                "strikes": [],
                "totals": {},
            }

        underlying_price = None
        if "underlying_price" in df.columns:
            valid_u = df["underlying_price"].drop_nulls()
            if not valid_u.is_empty():
                underlying_price = _clean_json_val(float(valid_u[0]))

        exp_summary_df = (
            df.group_by("expiration")
            .agg(
                [
                    pl.col("dte").first().alias("dte"),
                    pl.len().alias("contract_count"),
                    pl.col("volume").sum().alias("total_volume"),
                    pl.col("open_interest").sum().alias("total_oi"),
                    pl.col("volume").filter(pl.col("right") == "call").sum().alias("calls_volume"),
                    pl.col("volume").filter(pl.col("right") == "put").sum().alias("puts_volume"),
                    pl.col("open_interest").filter(pl.col("right") == "call").sum().alias("calls_oi"),
                    pl.col("open_interest").filter(pl.col("right") == "put").sum().alias("puts_oi"),
                ]
            )
            .sort("expiration")
        )
        exp_records = _df_to_records(exp_summary_df)
        avail_exps = [r["expiration"] for r in exp_records if r.get("expiration")]

        selected_exp = expiration
        if not selected_exp or selected_exp not in avail_exps:
            preferred = [r["expiration"] for r in exp_records if (r.get("dte") or 0) >= 2]
            selected_exp = preferred[0] if preferred else (avail_exps[0] if avail_exps else None)

        strikes_list: list[dict[str, Any]] = []
        selected_dte = None
        totals: dict[str, Any] = {}

        if selected_exp:
            exp_date_obj = datetime.date.fromisoformat(selected_exp)
            sub_df = df.filter(pl.col("expiration") == exp_date_obj).sort(["strike", "right"])
            sub_records = _df_to_records(sub_df)
            if sub_records:
                selected_dte = sub_records[0].get("dte")

            strikes_map: dict[float, dict[str, Any]] = {}
            for r in sub_records:
                st = float(r["strike"])
                if st not in strikes_map:
                    moneyness = None
                    if underlying_price and underlying_price > 0:
                        moneyness = round(((st - underlying_price) / underlying_price) * 100, 2)
                    strikes_map[st] = {
                        "strike": st,
                        "is_atm": False,
                        "moneyness_pct": moneyness,
                        "call": None,
                        "put": None,
                    }
                right = str(r.get("right", "")).lower()
                if right in ("call", "put"):
                    strikes_map[st][right] = r

            sorted_strikes = sorted(strikes_map.keys())
            if sorted_strikes and underlying_price is not None:
                atm_st = min(sorted_strikes, key=lambda s: abs(s - underlying_price))
                if atm_st in strikes_map:
                    strikes_map[atm_st]["is_atm"] = True

            strikes_list = [strikes_map[st] for st in sorted_strikes]

            # Day-wide session totals across all expirations on this trade date
            session_c_vol = sum((r.get("calls_volume") or 0) for r in exp_records)
            session_p_vol = sum((r.get("puts_volume") or 0) for r in exp_records)
            session_tot_vol = session_c_vol + session_p_vol
            session_c_oi = sum((r.get("calls_oi") or 0) for r in exp_records)
            session_p_oi = sum((r.get("puts_oi") or 0) for r in exp_records)
            session_tot_oi = session_c_oi + session_p_oi
            session_pc_vol = round(session_p_vol / session_c_vol, 3) if session_c_vol > 0 else None
            session_pc_oi = round(session_p_oi / session_c_oi, 3) if session_c_oi > 0 else None

            # Active expiration totals
            exp_c_vol = sum((r["call"].get("volume") or 0) for r in strikes_list if r["call"])
            exp_p_vol = sum((r["put"].get("volume") or 0) for r in strikes_list if r["put"])
            exp_tot_vol = exp_c_vol + exp_p_vol
            exp_c_oi = sum((r["call"].get("open_interest") or 0) for r in strikes_list if r["call"])
            exp_p_oi = sum((r["put"].get("open_interest") or 0) for r in strikes_list if r["put"])
            exp_tot_oi = exp_c_oi + exp_p_oi

            totals = {
                "session_call_volume": session_c_vol,
                "session_put_volume": session_p_vol,
                "session_total_volume": session_tot_vol,
                "session_call_oi": session_c_oi,
                "session_put_oi": session_p_oi,
                "session_total_oi": session_tot_oi,
                "session_put_call_volume_ratio": session_pc_vol,
                "session_put_call_oi_ratio": session_pc_oi,
                "call_volume": session_c_vol,
                "put_volume": session_p_vol,
                "total_volume": session_tot_vol,
                "call_oi": session_c_oi,
                "put_oi": session_p_oi,
                "total_oi": session_tot_oi,
                "put_call_volume_ratio": session_pc_vol,
                "put_call_oi_ratio": session_pc_oi,
                "active_exp_call_volume": exp_c_vol,
                "active_exp_put_volume": exp_p_vol,
                "active_exp_total_volume": exp_tot_vol,
                "active_exp_call_oi": exp_c_oi,
                "active_exp_put_oi": exp_p_oi,
                "active_exp_total_oi": exp_tot_oi,
            }

        return {
            "symbol": sym,
            "trade_date": td_str,
            "available_trade_dates": available_dates,
            "underlying_price": underlying_price,
            "expirations": exp_records,
            "selected_expiration": selected_exp,
            "selected_dte": selected_dte,
            "strikes": strikes_list,
            "totals": totals,
        }

    def fetch_from_gdrive(self, symbol: str, year: int | None = None) -> dict[str, Any]:
        """Explicitly pull `symbol` (and optional `year`) from Google Drive into local cache."""
        sym = validate_symbol(symbol)
        years = [year] if year is not None else None
        if self.loader.gdrive is None:
            raise RuntimeError("Google Drive folder ID is not configured.")
        files = self.loader.gdrive.download_symbol_parquets(sym, self.data_dir, years=years)
        return {
            "symbol": sym,
            "pulled_years": years or "all",
            "local_files": [p.name for p in files],
        }


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>ThetaData Options & Greeks Analytics Terminal</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {
      --bg-primary: #0b0f19;
      --bg-panel: #111827;
      --bg-card: #1e293b;
      --border: #334155;
      --border-subtle: #1e293b;
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
      --accent: #38bdf8;
      --accent-green: #22c55e;
      --accent-red: #ef4444;
      --accent-purple: #a855f7;
      --accent-amber: #f59e0b;
      --call-color: #22c55e;
      --put-color: #ef4444;
      --itm-call-bg: rgba(34, 197, 94, 0.08);
      --itm-put-bg: rgba(239, 68, 68, 0.08);
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg-primary);
      color: var(--text-main);
      font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, monospace;
      font-size: 13px;
      line-height: 1.4;
    }
    header {
      background: var(--bg-panel);
      border-bottom: 1px solid var(--border);
      padding: 10px 20px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 15px;
      font-weight: 700;
      color: var(--accent);
    }
    .badge {
      background: rgba(56, 189, 248, 0.15);
      color: var(--accent);
      border: 1px solid rgba(56, 189, 248, 0.35);
      padding: 2px 8px;
      border-radius: 999px;
      font-size: 11px;
      font-weight: 600;
    }
    .badge-green {
      background: rgba(34, 197, 94, 0.15);
      color: var(--accent-green);
      border: 1px solid rgba(34, 197, 94, 0.35);
    }
    .toolbar {
      background: var(--bg-panel);
      border-bottom: 1px solid var(--border);
      padding: 10px 20px;
      display: flex;
      flex-wrap: wrap;
      align-items: flex-end;
      gap: 12px;
    }
    .field {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .field label {
      font-size: 10.5px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      font-weight: 600;
    }
    select, input {
      background: var(--bg-card);
      color: var(--text-main);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 6px 10px;
      font-size: 12.5px;
      min-width: 110px;
      outline: none;
    }
    select:focus, input:focus {
      border-color: var(--accent);
    }
    button {
      background: var(--accent);
      color: #020617;
      border: none;
      border-radius: 6px;
      padding: 6px 12px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
      transition: opacity 0.15s, background 0.15s;
      white-space: nowrap;
    }
    button:hover { opacity: 0.9; }
    button.secondary {
      background: var(--bg-card);
      color: var(--text-main);
      border: 1px solid var(--border);
    }
    button.secondary:hover {
      background: #334155;
    }
    button.secondary.active {
      border-color: var(--accent);
      color: var(--accent);
      background: rgba(56, 189, 248, 0.12);
    }
    .tabs {
      display: flex;
      gap: 6px;
      padding: 8px 20px 0;
      border-bottom: 1px solid var(--border);
      background: var(--bg-primary);
    }
    .tab-btn {
      background: transparent;
      color: var(--text-muted);
      border: none;
      border-bottom: 2px solid transparent;
      border-radius: 0;
      padding: 8px 14px;
      font-size: 12.5px;
      font-weight: 600;
    }
    .tab-btn:hover {
      color: var(--text-main);
    }
    .tab-btn.active {
      color: var(--accent);
      border-bottom-color: var(--accent);
    }

    /* Contextual Navigation Breadcrumb Banner */
    .nav-context-banner {
      margin: 12px 20px 0 20px;
      padding: 9px 14px;
      background: rgba(30, 41, 59, 0.7);
      border: 1px solid rgba(56, 189, 248, 0.35);
      border-radius: 8px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .banner-content {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }
    .banner-badge {
      background: rgba(56, 189, 248, 0.2);
      color: var(--accent);
      padding: 2px 7px;
      border-radius: 4px;
      font-size: 10.5px;
      font-weight: 700;
      text-transform: uppercase;
    }
    #banner-text {
      font-weight: 700;
      color: #fff;
    }
    .banner-sub {
      color: var(--text-muted);
      font-size: 11.5px;
    }
    .btn-banner-action {
      background: var(--accent);
      color: #0b0f19;
      font-weight: 700;
      font-size: 11.5px;
      padding: 5px 12px;
    }
    .btn-banner-close {
      background: transparent;
      color: var(--text-muted);
      font-size: 14px;
      padding: 2px 6px;
    }
    .btn-banner-close:hover {
      color: #fff;
    }

    /* KPI Summary Strips */
    .kpi-strip {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(145px, 1fr));
      gap: 10px;
      padding: 12px 20px;
    }
    .kpi-card {
      background: var(--bg-panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 9px 12px;
    }
    .kpi-title {
      font-size: 10.5px;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }
    .kpi-value {
      font-size: 17px;
      font-weight: 700;
      margin-top: 3px;
    }
    .kpi-sub {
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 2px;
    }

    /* Option Chain Expiration Pills Bar */
    .chain-exp-container {
      padding: 10px 20px 4px 20px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .chain-exp-header {
      font-size: 11px;
      font-weight: 600;
      color: var(--text-muted);
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .exp-pills-bar {
      display: flex;
      gap: 8px;
      overflow-x: auto;
      padding-bottom: 6px;
      scrollbar-width: thin;
    }
    .exp-pills-bar::-webkit-scrollbar {
      height: 6px;
    }
    .exp-pills-bar::-webkit-scrollbar-thumb {
      background: #334155;
      border-radius: 3px;
    }
    .exp-pill {
      background: var(--bg-panel);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 5px 10px;
      display: flex;
      align-items: center;
      gap: 6px;
      cursor: pointer;
      font-size: 11.5px;
      white-space: nowrap;
      transition: all 0.15s;
    }
    .exp-pill:hover {
      border-color: var(--accent);
      background: rgba(56, 189, 248, 0.08);
    }
    .exp-pill.active {
      border-color: var(--accent);
      background: rgba(56, 189, 248, 0.18);
      color: #fff;
      font-weight: 700;
    }
    .exp-pill .dte-tag {
      background: rgba(255, 255, 255, 0.08);
      padding: 1px 5px;
      border-radius: 4px;
      font-size: 10px;
      color: var(--accent);
      font-weight: 600;
    }
    .exp-pill .vol-tag {
      font-size: 10px;
      color: var(--text-muted);
    }

    /* Dual Option Chain Table */
    .chain-table-wrapper {
      padding: 6px 20px 20px;
      overflow-x: auto;
    }
    .chain-table {
      width: 100%;
      border-collapse: collapse;
      background: var(--bg-panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
      font-size: 11.5px;
    }
    .chain-table th, .chain-table td {
      padding: 6px 8px;
      border-bottom: 1px solid #1e293b;
      text-align: right;
      white-space: nowrap;
    }
    .chain-table thead tr:first-child th {
      background: #0f172a;
      font-size: 11.5px;
      font-weight: 700;
      text-align: center;
      border-bottom: 2px solid var(--border);
      padding: 8px 10px;
    }
    .chain-table thead tr:nth-child(2) th {
      background: var(--bg-card);
      font-size: 10.5px;
      color: var(--text-muted);
      position: sticky;
      top: 0;
      z-index: 2;
    }
    .chain-table .th-calls {
      color: var(--call-color);
      border-right: 1px solid var(--border);
    }
    .chain-table .th-puts {
      color: var(--put-color);
      border-left: 1px solid var(--border);
    }
    .chain-table .th-strike {
      color: var(--accent);
      background: #182234 !important;
    }
    .chain-table td.td-strike {
      text-align: center;
      font-weight: 700;
      font-size: 12.5px;
      color: #fff;
      background: #141e30;
      border-left: 1px solid #334155;
      border-right: 1px solid #334155;
      position: relative;
    }
    .chain-table td.td-strike.strike-atm {
      background: rgba(56, 189, 248, 0.22);
      color: var(--accent);
      border-top: 1px solid var(--accent);
      border-bottom: 1px solid var(--accent);
    }
    .atm-badge {
      display: inline-block;
      font-size: 9px;
      background: var(--accent);
      color: #0b0f19;
      padding: 0 4px;
      border-radius: 3px;
      margin-left: 4px;
      font-weight: 800;
      vertical-align: middle;
    }
    .chain-table tr:hover td {
      background: rgba(56, 189, 248, 0.08);
    }
    .chain-table tr:hover td.td-strike {
      background: #1e293b;
    }
    .chain-table tr.itm-call td.call-cell {
      background: var(--itm-call-bg);
    }
    .chain-table tr.itm-put td.put-cell {
      background: var(--itm-put-bg);
    }
    .btn-action-view {
      background: rgba(56, 189, 248, 0.15);
      border: 1px solid rgba(56, 189, 248, 0.3);
      color: var(--accent);
      padding: 2px 7px;
      border-radius: 4px;
      font-size: 10.5px;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 3px;
      transition: background 0.15s;
    }
    .btn-action-view:hover {
      background: var(--accent);
      color: #0b0f19;
    }
    .call-cell.clickable:hover, .put-cell.clickable:hover {
      background: rgba(56, 189, 248, 0.22) !important;
      cursor: pointer;
    }

    /* Pulse animation when navigated from contract view */
    @keyframes pulseHighlight {
      0% { background: rgba(56, 189, 248, 0.4); }
      50% { background: rgba(56, 189, 248, 0.2); }
      100% { background: transparent; }
    }
    .chain-table tr.highlight-pulse td {
      animation: pulseHighlight 2.5s ease-out;
    }

    /* Charts Layout */
    .grid-charts {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 14px;
      padding: 0 20px 20px;
    }
    @media (max-width: 1100px) {
      .grid-charts { grid-template-columns: minmax(0, 1fr); }
    }
    .chart-box {
      background: var(--bg-panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 0;
      min-height: 385px;
      position: relative;
      overflow: hidden;
      min-width: 0;
      box-sizing: border-box;
    }
    .chart-box.full-width {
      grid-column: 1 / -1;
      min-height: 425px;
    }
    .table-container {
      padding: 0 20px 24px;
      overflow-x: auto;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      background: var(--bg-panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
      font-size: 12px;
    }
    table th, table td {
      padding: 7px 10px;
      border-bottom: 1px solid #1e293b;
      text-align: right;
      white-space: nowrap;
    }
    table th {
      background: var(--bg-card);
      color: var(--text-muted);
      font-weight: 600;
      position: sticky;
      top: 0;
    }
    table th:first-child, table td:first-child { text-align: left; }
    table tr:hover td { background: rgba(56, 189, 248, 0.05); }

    .status-bar {
      font-size: 12px;
      color: var(--text-muted);
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .hidden { display: none !important; }

    /* Searchable Symbol Combobox */
    .symbol-combobox {
      position: relative;
      width: 250px;
    }
    .symbol-input-wrapper {
      position: relative;
      display: flex;
      align-items: center;
    }
    .symbol-input-wrapper input {
      width: 100%;
      padding-left: 28px;
      padding-right: 28px;
      cursor: pointer;
      font-weight: 500;
    }
    .symbol-input-wrapper input:focus {
      cursor: text;
      border-color: var(--accent);
      box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.2);
    }
    .symbol-search-icon {
      position: absolute;
      left: 9px;
      font-size: 11px;
      color: var(--text-muted);
      pointer-events: none;
    }
    .symbol-arrow-icon {
      position: absolute;
      right: 10px;
      font-size: 9px;
      color: var(--text-muted);
      cursor: pointer;
      transition: transform 0.2s;
    }
    .symbol-combobox.open .symbol-arrow-icon {
      transform: rotate(180deg);
      color: var(--accent);
    }
    .symbol-dropdown {
      position: absolute;
      top: calc(100% + 4px);
      left: 0;
      width: 340px;
      max-height: 380px;
      background: #0f172a;
      border: 1px solid var(--border);
      border-radius: 8px;
      box-shadow: 0 16px 36px rgba(0, 0, 0, 0.75);
      overflow-y: auto;
      z-index: 9999;
      display: none;
    }
    .symbol-dropdown::-webkit-scrollbar {
      width: 6px;
    }
    .symbol-dropdown::-webkit-scrollbar-track {
      background: #0f172a;
    }
    .symbol-dropdown::-webkit-scrollbar-thumb {
      background: #334155;
      border-radius: 3px;
    }
    .symbol-combobox.open .symbol-dropdown {
      display: block;
    }
    .symbol-dropdown-header {
      padding: 6px 10px;
      font-size: 10.5px;
      color: var(--text-muted);
      border-bottom: 1px solid #1e293b;
      background: rgba(15, 23, 42, 0.95);
      position: sticky;
      top: 0;
      z-index: 2;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .symbol-item {
      padding: 7px 10px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      cursor: pointer;
      border-bottom: 1px solid rgba(255, 255, 255, 0.03);
      transition: background 0.1s;
    }
    .symbol-item:hover, .symbol-item.highlighted {
      background: rgba(56, 189, 248, 0.15);
    }
    .symbol-item.selected {
      background: rgba(56, 189, 248, 0.22);
      border-left: 3px solid var(--accent);
    }
    .symbol-item-left {
      display: flex;
      align-items: baseline;
      gap: 6px;
      overflow: hidden;
    }
    .symbol-ticker {
      font-weight: 700;
      color: var(--accent);
      font-size: 13px;
    }
    .symbol-name {
      font-size: 11.5px;
      color: var(--text-muted);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      max-width: 170px;
    }
    .symbol-tag {
      font-size: 10px;
      padding: 1px 6px;
      border-radius: 999px;
      font-weight: 600;
      white-space: nowrap;
    }
    .symbol-tag.local {
      background: rgba(34, 197, 94, 0.15);
      color: var(--accent-green);
      border: 1px solid rgba(34, 197, 94, 0.3);
    }
    .symbol-tag.cloud {
      background: rgba(148, 163, 184, 0.12);
      color: var(--text-muted);
      border: 1px solid rgba(148, 163, 184, 0.25);
    }
    .symbol-no-match {
      padding: 20px;
      text-align: center;
      color: var(--text-muted);
      font-size: 12px;
    }

    /* TradingView Chart Interactions */
    .js-plotly-plot {
      user-select: none;
      -webkit-user-select: none;
    }
    .js-plotly-plot .gtitle {
      font-weight: 600 !important;
    }
    .js-plotly-plot .draglayer .nsewdrag {
      cursor: grab !important;
    }
    .js-plotly-plot .draglayer .nsewdrag:active {
      cursor: grabbing !important;
    }
    .chart-zoom-overlay {
      position: absolute;
      top: 50%;
      left: 50%;
      transform: translate(-50%, -50%);
      background: rgba(15, 23, 42, 0.90);
      border: 1px solid rgba(56, 189, 248, 0.45);
      color: #f8fafc;
      padding: 8px 16px;
      border-radius: 8px;
      font-size: 12px;
      font-weight: 500;
      pointer-events: none;
      opacity: 0;
      transition: opacity 0.2s ease;
      z-index: 25;
      box-shadow: 0 4px 16px rgba(0, 0, 0, 0.5);
      user-select: none;
      white-space: nowrap;
    }
    .chart-zoom-overlay.visible {
      opacity: 1;
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <span>⚡ ThetaData Options & Greeks Explorer</span>
      <span class="badge">Polars Lazy Engine</span>
      <span class="badge badge-green" id="cache-badge">Local + GDrive Hybrid</span>
    </div>
    <div class="status-bar">
      <span id="status-msg">Ready</span>
      <button class="secondary" onclick="pullFromGDrive()" title="Pull missing Parquet files for this symbol from Google Drive">☁ Pull Symbol from Google Drive</button>
    </div>
  </header>

  <!-- TOP NAVIGATION TABS -->
  <div class="tabs">
    <button class="tab-btn active" id="tab-btn-chain" onclick="switchTab('chain')">📅 Daily Option Chain (Snapshot by Date)</button>
    <button class="tab-btn" id="tab-btn-lifecycle" onclick="switchTab('lifecycle')">📈 Contract Lifecycle (Deep Dive Time Series)</button>
    <button class="tab-btn" id="tab-btn-smile" onclick="switchTab('smile')">🌐 IV Smile & Strike Cross-Section</button>
    <button class="tab-btn" id="tab-btn-table" onclick="switchTab('table')">📋 Raw Historical Data & CSV Export</button>
  </div>

  <!-- TOOLBAR -->
  <div class="toolbar">
    <!-- Symbol Combobox (Always visible) -->
    <div class="field">
      <label>Symbol (S&P 500)</label>
      <div class="symbol-combobox" id="symbol-combobox">
        <div class="symbol-input-wrapper">
          <span class="symbol-search-icon">🔍</span>
          <input 
            type="text" 
            id="symbol-search-input" 
            placeholder="Search ticker / company..." 
            autocomplete="off" 
            spellcheck="false" 
          />
          <span class="symbol-arrow-icon" id="symbol-arrow-icon">▼</span>
        </div>
        <select id="sel-symbol" onchange="onSymbolChange()" style="display:none;"></select>
        <div class="symbol-dropdown" id="symbol-dropdown">
          <div class="symbol-dropdown-header" id="symbol-dropdown-header">
            <span id="symbol-count-badge">576 Symbols</span>
            <span style="color:var(--text-muted); font-size:10px;">Type to search • ↑↓ to navigate</span>
          </div>
          <div id="symbol-dropdown-list"></div>
        </div>
      </div>
    </div>

    <!-- CONTROLS FOR TAB 1: DAILY OPTION CHAIN -->
    <div class="field" id="fld-chain-date">
      <label>Snapshot Trade Date</label>
      <div style="display:flex; gap:4px; align-items:center;">
        <button class="secondary" onclick="stepTradeDate(-1)" title="Previous Trading Day">◀</button>
        <select id="sel-chain-date" onchange="onChainDateChange()"></select>
        <button class="secondary" onclick="stepTradeDate(1)" title="Next Trading Day">▶</button>
        <button class="secondary" onclick="jumpToLatestDate()" title="Jump to Latest Session">⚡ Latest</button>
      </div>
    </div>

    <div class="field" id="fld-chain-exp">
      <label>Expiration Date</label>
      <select id="sel-chain-exp" onchange="onChainExpSelectChange()"></select>
    </div>

    <div class="field" id="fld-strike-range">
      <label>Strike Filter</label>
      <select id="sel-strike-range" onchange="filterChainStrikes()">
        <option value="10">Near Spot (±10%)</option>
        <option value="25" selected>Active Range (±25%)</option>
        <option value="50">Broad (±50%)</option>
        <option value="all">All Available Strikes</option>
      </select>
    </div>

    <div class="field" id="fld-strike-search">
      <label>Quick Strike</label>
      <input type="text" id="input-strike-search" placeholder="e.g. 340" style="width:90px;" oninput="filterChainStrikes()" />
    </div>

    <!-- CONTROLS FOR TAB 2 & 4: CONTRACT LIFECYCLE & RAW TABLE -->
    <div class="field hidden" id="fld-year">
      <label>Year Filter</label>
      <select id="sel-year" onchange="onYearChange()"></select>
    </div>
    <div class="field hidden" id="fld-expiration">
      <label>Expiration Date</label>
      <select id="sel-expiration" onchange="onExpirationChange()"></select>
    </div>
    <div class="field hidden" id="fld-right">
      <label>Option Type</label>
      <select id="sel-right" onchange="loadContractSeries()">
        <option value="call">CALL</option>
        <option value="put">PUT</option>
      </select>
    </div>
    <div class="field hidden" id="fld-strike">
      <label>Strike Price</label>
      <select id="sel-strike" onchange="loadContractSeries()"></select>
    </div>
    <div class="field hidden" id="fld-quick">
      <label>Quick Pick</label>
      <div style="display:flex; gap:5px;">
        <button class="secondary" onclick="pickATM()">ATM</button>
        <button class="secondary" onclick="pickMaxOI()">Max OI</button>
      </div>
    </div>

    <!-- JUMP TO CHAIN ON DATE (Visible on Lifecycle Tab) -->
    <div class="field hidden" id="fld-jump-chain">
      <label>Cross-Section at Date</label>
      <div style="display:flex; gap:4px;">
        <select id="sel-contract-date"></select>
        <button class="secondary" onclick="jumpFromContractToChain()" title="Inspect the full option chain across all strikes on this date">📅 View Day Chain</button>
      </div>
    </div>

    <!-- CONTROLS FOR TAB 3: SMILE -->
    <div class="field hidden" id="fld-tradedate">
      <label>Trade Date (Snapshot)</label>
      <select id="sel-tradedate" onchange="loadChainSmile()"></select>
    </div>
  </div>

  <!-- CONTEXTUAL BREADCRUMB BANNER -->
  <div id="nav-context-banner" class="nav-context-banner hidden">
    <div class="banner-content">
      <span class="banner-badge" id="banner-badge">Active Selection</span>
      <span id="banner-text">Viewing AAPL 2026-09-21 $340 CALL</span>
      <span class="banner-sub" id="banner-sub">Selected from 2026-09-15 Option Chain</span>
    </div>
    <div style="display:flex; gap:8px; align-items:center;">
      <button class="btn-banner-action" id="btn-banner-action" onclick="bannerActionClick()">⬅ Back to 2026-09-15 Option Chain</button>
      <button class="btn-banner-close" onclick="closeBanner()">✕</button>
    </div>
  </div>

  <!-- ========================================== -->
  <!-- TAB 1: DAILY OPTION CHAIN (SNAPSHOT VIEW) -->
  <!-- ========================================== -->
  <div id="view-chain">
    <!-- Market Snapshot KPIs -->
    <div class="kpi-strip">
      <div class="kpi-card">
        <div class="kpi-title">Underlying Spot</div>
        <div class="kpi-value" id="chain-kpi-spot" style="color:var(--accent)">$--</div>
        <div class="kpi-sub" id="chain-kpi-atm">ATM Strike: $--</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-title">Expirations Available</div>
        <div class="kpi-value" id="chain-kpi-exps-count">--</div>
        <div class="kpi-sub" id="chain-kpi-dte-range">DTE Range: --</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-title">Active Expiration</div>
        <div class="kpi-value" id="chain-kpi-selected-exp">--</div>
        <div class="kpi-sub" id="chain-kpi-strikes-count">-- strikes</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-title">Session Option Volume</div>
        <div class="kpi-value" id="chain-kpi-vol">--</div>
        <div class="kpi-sub" id="chain-kpi-pc-vol">P/C Volume Ratio: --</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-title">Open Interest</div>
        <div class="kpi-value" id="chain-kpi-oi" style="color:var(--accent-green)">--</div>
        <div class="kpi-sub" id="chain-kpi-pc-oi">P/C OI Ratio: --</div>
      </div>
    </div>

    <!-- Expirations Horizontal Carousel / Pills -->
    <div class="chain-exp-container">
      <div class="chain-exp-header">
        <span>Available Expirations for <b id="chain-exp-sym-label">AAPL</b> on <b id="chain-exp-date-label">--</b> (Click to inspect strikes):</span>
        <span style="font-size:10.5px; color:var(--text-muted)">Scroll horizontally for longer-term expirations →</span>
      </div>
      <div class="exp-pills-bar" id="exp-pills-bar">
        <!-- Rendered dynamically -->
      </div>
    </div>

    <!-- Dual-Sided Option Chain Table (Calls | Strike | Puts) -->
    <div class="chain-table-wrapper">
      <table class="chain-table" id="chain-table">
        <thead>
          <tr>
            <th colspan="7" class="th-calls">CALLS</th>
            <th colspan="2" class="th-strike" id="th-strike-title">STRIKE & SPOT</th>
            <th colspan="7" class="th-puts">PUTS</th>
          </tr>
          <tr>
            <!-- Calls Subheaders -->
            <th title="Click to view lifecycle">Analyze</th>
            <th>Delta</th>
            <th>IV (%)</th>
            <th>Bid / Ask</th>
            <th style="color:var(--accent)">Mid ($)</th>
            <th>Volume</th>
            <th>Open Int</th>

            <!-- Strike Subheaders -->
            <th style="background:#182234; color:var(--accent)">Strike</th>
            <th style="background:#182234">% vs Spot</th>

            <!-- Puts Subheaders -->
            <th style="color:var(--accent)">Mid ($)</th>
            <th>Bid / Ask</th>
            <th>IV (%)</th>
            <th>Delta</th>
            <th>Volume</th>
            <th>Open Int</th>
            <th title="Click to view lifecycle">Analyze</th>
          </tr>
        </thead>
        <tbody id="chain-table-body">
          <!-- Rendered dynamically -->
        </tbody>
      </table>
    </div>
  </div>

  <!-- ========================================== -->
  <!-- TAB 2: CONTRACT LIFECYCLE (DEEP DIVE VIEW) -->
  <!-- ========================================== -->
  <div id="view-lifecycle" class="hidden">
    <!-- Single Contract KPI Strip -->
    <div class="kpi-strip" id="kpi-strip">
      <div class="kpi-card">
        <div class="kpi-title">Contract</div>
        <div class="kpi-value" id="kpi-contract">--</div>
        <div class="kpi-sub" id="kpi-days">-- trading days</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-title">Latest Option Mid / Close</div>
        <div class="kpi-value" id="kpi-price">--</div>
        <div class="kpi-sub" id="kpi-bidask">Bid/Ask: --</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-title">Underlying Spot</div>
        <div class="kpi-value" id="kpi-spot">--</div>
        <div class="kpi-sub" id="kpi-moneyness">Moneyness: --</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-title">Implied Volatility</div>
        <div class="kpi-value" id="kpi-iv" style="color:var(--accent-amber)">--</div>
        <div class="kpi-sub" id="kpi-dte">DTE: --</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-title">Delta (Δ) / Gamma (Γ)</div>
        <div class="kpi-value" id="kpi-delta" style="color:var(--accent)">--</div>
        <div class="kpi-sub" id="kpi-gamma">Gamma: --</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-title">Theta (Θ) / Vega (1%)</div>
        <div class="kpi-value" id="kpi-theta" style="color:var(--accent-red)">--</div>
        <div class="kpi-sub" id="kpi-vega">Vega/%: --</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-title">Open Interest / Vol</div>
        <div class="kpi-value" id="kpi-oi" style="color:var(--accent-green)">--</div>
        <div class="kpi-sub" id="kpi-vol">Peak Vol: --</div>
      </div>
    </div>

    <!-- Contract Lifecycle Charts -->
    <div class="grid-charts">
      <div class="chart-box full-width" id="chart-price" style="min-height:425px;"></div>
      <div class="chart-box" id="chart-greeks-1"></div>
      <div class="chart-box" id="chart-greeks-2"></div>
      <div class="chart-box" id="chart-iv-oi"></div>
      <div class="chart-box" id="chart-higher-greeks"></div>
    </div>
  </div>

  <!-- ========================================== -->
  <!-- TAB 3: IV SMILE & STRIKE CROSS-SECTION -->
  <!-- ========================================== -->
  <div id="view-smile" class="grid-charts hidden">
    <div class="chart-box" id="chart-smile-iv"></div>
    <div class="chart-box" id="chart-smile-delta"></div>
    <div class="chart-box" id="chart-smile-gex"></div>
    <div class="chart-box" id="chart-smile-oi"></div>
  </div>

  <!-- ========================================== -->
  <!-- TAB 4: RAW DATA TABLE & CSV EXPORT -->
  <!-- ========================================== -->
  <div id="view-table" class="table-container hidden">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
      <span style="color:var(--text-muted)">Daily EOD + Greeks records. Click any row's <b>📅 Chain</b> to inspect that day's full market snapshot.</span>
      <button onclick="exportCSV()">⬇ Download CSV</button>
    </div>
    <table id="data-table">
      <thead>
        <tr>
          <th>Date</th><th>Day Chain</th><th>DTE</th><th>Spot ($)</th><th>Bid</th><th>Ask</th><th>Mid</th><th>Close</th>
          <th>Vol</th><th>Open Int</th><th>IV (%)</th><th>Delta</th><th>Gamma</th><th>Theta</th>
          <th>Vega/1%</th><th>Rho/1%</th><th>Vanna</th><th>Charm</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <script>
    let currentTab = 'chain';
    let allSymbols = [];
    let highlightedIndex = -1;
    let filteredSymbols = [];

    // State
    let currentMeta = null;
    let currentChainData = null;
    let currentContractsMeta = null;
    let currentSeries = [];

    // Context tracking for bidirectional jumps
    let originChainContext = null; // { symbol, tradeDate, expiration }
    let lastContractContext = null; // { symbol, expiration, strike, right }

    const plotLayoutBase = {
      paper_bgcolor: '#111827',
      plot_bgcolor: '#111827',
      font: { color: '#e2e8f0', size: 11 },
      margin: { l: 65, r: 65, t: 84, b: 45 },
      dragmode: 'pan',
      title: {
        x: 0.01,
        xanchor: 'left',
        y: 0.98,
        yref: 'container',
        yanchor: 'top',
        font: { size: 13, color: '#f8fafc' },
        pad: { l: 0, t: 4, b: 0 }
      },
      legend: {
        orientation: 'h',
        y: 1.02,
        yanchor: 'bottom',
        x: 0.01,
        xanchor: 'left',
        font: { size: 11, color: '#94a3b8' }
      },
      xaxis: { gridcolor: '#1e293b', zerolinecolor: '#334155' },
      yaxis: { gridcolor: '#1e293b', zerolinecolor: '#334155' },
      hovermode: 'x unified'
    };

    const plotConfig = {
      responsive: true,
      displaylogo: false,
      showAxisDragHandles: false,
      modeBarButtonsToRemove: ['select2d', 'lasso2d'],
    };

    function parsePlotlyDate(val) {
      if (typeof val === 'number') return val;
      if (!val) return NaN;
      if (typeof val === 'string') {
        const s = val.trim().replace(' ', 'T');
        const iso = (s.includes('Z') || s.includes('+') || (s.length > 10 && s.slice(10).includes('-'))) ? s : s + 'Z';
        const t = Date.parse(iso);
        if (!isNaN(t)) return t;
        return Date.parse(val);
      }
      if (val instanceof Date) return val.getTime();
      return Number(val);
    }

    function formatPlotlyDate(ms) {
      const d = new Date(ms);
      if (isNaN(d.getTime())) return '';
      const pad = n => String(n).padStart(2, '0');
      const yyyy = d.getUTCFullYear();
      const mm = pad(d.getUTCMonth() + 1);
      const dd = pad(d.getUTCDate());
      const hh = pad(d.getUTCHours());
      const mi = pad(d.getUTCMinutes());
      const ss = pad(d.getUTCSeconds());
      return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
    }

    function showZoomHint(gd) {
      let toast = gd.querySelector('.chart-zoom-overlay');
      if (!toast) {
        toast = document.createElement('div');
        toast.className = 'chart-zoom-overlay';
        const isMac = (navigator.platform || '').toUpperCase().includes('MAC') || (navigator.userAgent || '').includes('Mac');
        toast.textContent = `Hold ${isMac ? '⌘' : 'Ctrl'} + scroll to zoom chart`;
        gd.appendChild(toast);
      }
      toast.classList.add('visible');
      clearTimeout(gd._zoomHintTimeout);
      gd._zoomHintTimeout = setTimeout(() => {
        toast.classList.remove('visible');
      }, 900);
    }

    function attachTradingViewInteractions(gd) {
      if (!gd || gd._tv_attached) return;
      gd._tv_attached = true;

      if (window.ResizeObserver && !gd._tv_ro) {
        gd._tv_ro = new ResizeObserver(() => {
          if (gd.offsetWidth > 0 && gd.offsetHeight > 0 && gd._fullLayout) {
            Plotly.Plots.resize(gd);
          }
        });
        gd._tv_ro.observe(gd);
      }

      let dragCtx = null;
      let rafId = null;

      gd.addEventListener('mousemove', function(e) {
        if (dragCtx) return;
        if (!gd._fullLayout) return;

        const rect = gd.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const w = rect.width;
        const h = rect.height;

        const ml = gd._fullLayout.margin ? gd._fullLayout.margin.l : 65;
        const mr = gd._fullLayout.margin ? gd._fullLayout.margin.r : 65;
        const mb = gd._fullLayout.margin ? gd._fullLayout.margin.b : 45;
        const mt = gd._fullLayout.margin ? gd._fullLayout.margin.t : 84;

        const hasY2 = !!gd._fullLayout.yaxis2;

        if (y >= h - mb - 5 && x >= ml && x <= w - mr) {
          gd.style.cursor = 'ew-resize';
        } else if (x <= ml + 5 && y >= mt && y <= h - mb) {
          gd.style.cursor = 'ns-resize';
        } else if (hasY2 && x >= w - mr - 5 && y >= mt && y <= h - mb) {
          gd.style.cursor = 'ns-resize';
        } else {
          gd.style.cursor = '';
        }
      });

      gd.addEventListener('mousedown', function(e) {
        if (e.button !== 0) return;
        if (!gd._fullLayout) return;

        const rect = gd.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const w = rect.width;
        const h = rect.height;

        const ml = gd._fullLayout.margin ? gd._fullLayout.margin.l : 65;
        const mr = gd._fullLayout.margin ? gd._fullLayout.margin.r : 65;
        const mb = gd._fullLayout.margin ? gd._fullLayout.margin.b : 45;
        const mt = gd._fullLayout.margin ? gd._fullLayout.margin.t : 84;

        const hasY2 = !!gd._fullLayout.yaxis2;

        let axisKey = null;
        if (y >= h - mb - 5 && x >= ml && x <= w - mr) {
          axisKey = 'xaxis';
        } else if (x <= ml + 5 && y >= mt && y <= h - mb) {
          axisKey = 'yaxis';
        } else if (hasY2 && x >= w - mr - 5 && y >= mt && y <= h - mb) {
          axisKey = 'yaxis2';
        }

        if (!axisKey) return;

        e.stopPropagation();
        e.stopImmediatePropagation();
        e.preventDefault();

        const ax = gd._fullLayout[axisKey];
        if (!ax || !ax.range) return;

        const isDate = (axisKey === 'xaxis' && (ax.type === 'date' || (typeof ax.range[0] === 'string' && ax.range[0].includes('-'))));
        let startMin, startMax;
        if (isDate) {
          startMin = parsePlotlyDate(ax.range[0]);
          startMax = parsePlotlyDate(ax.range[1]);
        } else {
          startMin = Number(ax.range[0]);
          startMax = Number(ax.range[1]);
        }

        const startSpan = startMax - startMin;
        if (!startSpan || isNaN(startSpan)) return;

        let anchorVal;
        if (axisKey === 'xaxis') {
          const frac = Math.max(0, Math.min(1, (x - ml) / (w - ml - mr)));
          anchorVal = startMin + frac * startSpan;
        } else {
          const frac = Math.max(0, Math.min(1, (h - mb - y) / (h - mt - mb)));
          anchorVal = startMin + frac * startSpan;
        }

        dragCtx = {
          axisKey,
          isDate,
          startMin,
          startMax,
          startSpan,
          anchorVal,
          startX: e.clientX,
          startY: e.clientY
        };

        document.body.style.userSelect = 'none';
        document.body.style.cursor = (axisKey === 'xaxis') ? 'ew-resize' : 'ns-resize';

        window.addEventListener('mousemove', onMouseMove, { passive: false });
        window.addEventListener('mouseup', onMouseUp);
      }, true);

      function onMouseMove(e) {
        if (!dragCtx) return;
        e.preventDefault();
        e.stopPropagation();

        const dx = e.clientX - dragCtx.startX;
        const dy = e.clientY - dragCtx.startY;

        if (rafId) cancelAnimationFrame(rafId);
        rafId = requestAnimationFrame(() => {
          if (!dragCtx) return;
          let factor;
          if (dragCtx.axisKey === 'xaxis') {
            factor = Math.exp(-dx * 0.006);
          } else {
            factor = Math.exp(dy * 0.006);
          }
          factor = Math.max(0.01, Math.min(100, factor));

          const a = dragCtx.anchorVal;
          const newMin = a - (a - dragCtx.startMin) * factor;
          const newMax = a + (dragCtx.startMax - a) * factor;

          let rangeVal;
          if (dragCtx.isDate) {
            rangeVal = [
              formatPlotlyDate(newMin),
              formatPlotlyDate(newMax)
            ];
          } else {
            rangeVal = [newMin, newMax];
          }

          Plotly.relayout(gd, {
            [`${dragCtx.axisKey}.range`]: rangeVal,
            [`${dragCtx.axisKey}.autorange`]: false
          });
        });
      }

      function onMouseUp() {
        window.removeEventListener('mousemove', onMouseMove);
        window.removeEventListener('mouseup', onMouseUp);
        document.body.style.userSelect = '';
        document.body.style.cursor = '';
        if (rafId) {
          cancelAnimationFrame(rafId);
          rafId = null;
        }
        dragCtx = null;
      }

      gd.addEventListener('wheel', function(e) {
        if (!gd._fullLayout) return;

        if (!e.ctrlKey && !e.metaKey) {
          showZoomHint(gd);
          return;
        }

        e.preventDefault();
        e.stopPropagation();

        const rect = gd.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const w = rect.width;
        const h = rect.height;

        const ml = gd._fullLayout.margin ? gd._fullLayout.margin.l : 65;
        const mr = gd._fullLayout.margin ? gd._fullLayout.margin.r : 65;
        const mb = gd._fullLayout.margin ? gd._fullLayout.margin.b : 45;
        const mt = gd._fullLayout.margin ? gd._fullLayout.margin.t : 84;

        const hasY2 = !!gd._fullLayout.yaxis2;

        let targetAxis = 'xaxis';
        if (x <= ml + 5) {
          targetAxis = 'yaxis';
        } else if (hasY2 && x >= w - mr - 5) {
          targetAxis = 'yaxis2';
        } else if (e.shiftKey) {
          targetAxis = 'yaxis';
        }

        const ax = gd._fullLayout[targetAxis];
        if (!ax || !ax.range) return;

        const isDate = (targetAxis === 'xaxis' && (ax.type === 'date' || (typeof ax.range[0] === 'string' && ax.range[0].includes('-'))));

        gd._tv_pendingRange = gd._tv_pendingRange || {};
        let curMin, curMax;
        if (gd._tv_pendingRange[targetAxis]) {
          [curMin, curMax] = gd._tv_pendingRange[targetAxis];
        } else if (isDate) {
          curMin = parsePlotlyDate(ax.range[0]);
          curMax = parsePlotlyDate(ax.range[1]);
        } else {
          curMin = Number(ax.range[0]);
          curMax = Number(ax.range[1]);
        }

        const curSpan = curMax - curMin;
        if (!curSpan || isNaN(curSpan)) return;

        let rawDelta = e.deltaY;
        if (Math.abs(rawDelta) < 0.001 && Math.abs(e.deltaX) > 0.001) {
          rawDelta = e.deltaX;
        }
        let dy = rawDelta;
        if (e.deltaMode === 1) dy *= 16;
        else if (e.deltaMode === 2) dy *= 100;
        dy = Math.max(-60, Math.min(60, dy));
        const factor = Math.exp(dy * 0.0016);

        let anchor;
        if (targetAxis === 'xaxis') {
          const frac = Math.max(0, Math.min(1, (x - ml) / (w - ml - mr)));
          anchor = curMin + frac * curSpan;
        } else {
          const frac = Math.max(0, Math.min(1, (h - mb - y) / (h - mt - mb)));
          anchor = curMin + frac * curSpan;
        }

        const newMin = anchor - (anchor - curMin) * factor;
        const newMax = anchor + (curMax - anchor) * factor;
        gd._tv_pendingRange[targetAxis] = [newMin, newMax];

        if (gd._tv_wheelRaf) cancelAnimationFrame(gd._tv_wheelRaf);
        gd._tv_wheelRaf = requestAnimationFrame(() => {
          const pending = gd._tv_pendingRange[targetAxis];
          if (!pending) return;
          delete gd._tv_pendingRange[targetAxis];

          let rangeVal;
          if (isDate) {
            rangeVal = [
              formatPlotlyDate(pending[0]),
              formatPlotlyDate(pending[1])
            ];
          } else {
            rangeVal = [pending[0], pending[1]];
          }

          Plotly.relayout(gd, {
            [`${targetAxis}.range`]: rangeVal,
            [`${targetAxis}.autorange`]: false
          });
        });
      }, { passive: false });

      gd.addEventListener('dblclick', function(e) {
        if (!gd._fullLayout) return;
        const rect = gd.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const w = rect.width;
        const h = rect.height;

        const ml = gd._fullLayout.margin ? gd._fullLayout.margin.l : 65;
        const mr = gd._fullLayout.margin ? gd._fullLayout.margin.r : 65;
        const mb = gd._fullLayout.margin ? gd._fullLayout.margin.b : 45;
        const mt = gd._fullLayout.margin ? gd._fullLayout.margin.t : 84;

        const hasY2 = !!gd._fullLayout.yaxis2;

        if (y >= h - mb - 5 && x >= ml && x <= w - mr) {
          e.stopPropagation();
          e.preventDefault();
          Plotly.relayout(gd, { 'xaxis.autorange': true });
        } else if (x <= ml + 5) {
          e.stopPropagation();
          e.preventDefault();
          Plotly.relayout(gd, { 'yaxis.autorange': true });
        } else if (hasY2 && x >= w - mr - 5) {
          e.stopPropagation();
          e.preventDefault();
          Plotly.relayout(gd, { 'yaxis2.autorange': true });
        } else {
          e.stopPropagation();
          e.preventDefault();
          const resetObj = { 'xaxis.autorange': true, 'yaxis.autorange': true };
          if (hasY2) resetObj['yaxis2.autorange'] = true;
          Plotly.relayout(gd, resetObj);
        }
      }, true);
    }

    function renderPlot(domId, traces, layout = {}) {
      const mergedTitle = typeof layout.title === 'string'
        ? { text: layout.title }
        : (layout.title || {});

      const mergedLayout = {
        ...plotLayoutBase,
        ...layout,
        dragmode: 'pan',
        title: {
          ...plotLayoutBase.title,
          ...mergedTitle,
          font: { ...plotLayoutBase.title.font, ...(mergedTitle.font || {}) },
        },
        legend: {
          ...plotLayoutBase.legend,
          ...(layout.legend || {}),
          font: { ...plotLayoutBase.legend.font, ...(layout.legend && layout.legend.font ? layout.legend.font : {}) },
        },
        margin: {
          ...plotLayoutBase.margin,
          ...(layout.margin || {}),
        },
        xaxis: { ...plotLayoutBase.xaxis, ...(layout.xaxis || {}) },
        yaxis: { ...plotLayoutBase.yaxis, ...(layout.yaxis || {}) },
      };
      if (layout.yaxis2) {
        mergedLayout.yaxis2 = {
          ...plotLayoutBase.yaxis,
          ...layout.yaxis2
        };
      }
      return Plotly.newPlot(domId, traces, mergedLayout, plotConfig).then(gd => {
        attachTradingViewInteractions(gd);
        return gd;
      });
    }

    function setStatus(msg) {
      document.getElementById('status-msg').textContent = msg;
    }

    async function apiGet(path) {
      const res = await fetch(path);
      if (!res.ok) {
        const err = await res.text();
        throw new Error(err || res.statusText);
      }
      return await res.json();
    }

    function escapeHtml(str) {
      return (str || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    // ==========================================
    // SYMBOL COMBOBOX LOGIC
    // ==========================================
    function updateSymbolInputDisplay(sym) {
      const input = document.getElementById('symbol-search-input');
      if (!input) return;
      const item = allSymbols.find(s => s.symbol === sym);
      if (item) {
        const cacheTag = item.cached_count > 0 ? `[${item.cached_count}y local]` : `[☁ Cloud]`;
        input.value = `${item.symbol} - ${item.company_name.slice(0, 20)} ${cacheTag}`;
      } else {
        input.value = sym;
      }
    }

    function setupSymbolCombobox(defaultSym) {
      const combobox = document.getElementById('symbol-combobox');
      const input = document.getElementById('symbol-search-input');
      const arrow = document.getElementById('symbol-arrow-icon');

      updateSymbolInputDisplay(defaultSym);

      function openDropdown() {
        combobox.classList.add('open');
        renderSymbolList(input.value.trim());
      }

      function closeDropdown() {
        combobox.classList.remove('open');
        highlightedIndex = -1;
        const currentSym = document.getElementById('sel-symbol').value;
        updateSymbolInputDisplay(currentSym);
      }

      input.addEventListener('focus', () => {
        openDropdown();
        input.select();
      });

      input.addEventListener('click', () => {
        if (!combobox.classList.contains('open')) {
          openDropdown();
          input.select();
        }
      });

      arrow.addEventListener('click', (e) => {
        e.stopPropagation();
        if (combobox.classList.contains('open')) {
          closeDropdown();
        } else {
          openDropdown();
          input.focus();
          input.select();
        }
      });

      input.addEventListener('input', () => {
        if (!combobox.classList.contains('open')) {
          combobox.classList.add('open');
        }
        highlightedIndex = 0;
        renderSymbolList(input.value.trim());
      });

      input.addEventListener('keydown', (e) => {
        if (!combobox.classList.contains('open')) {
          if (e.key === 'ArrowDown' || e.key === 'Enter') {
            openDropdown();
            e.preventDefault();
          }
          return;
        }

        if (e.key === 'ArrowDown') {
          e.preventDefault();
          if (filteredSymbols.length > 0) {
            highlightedIndex = (highlightedIndex + 1) % filteredSymbols.length;
            updateHighlight();
          }
        } else if (e.key === 'ArrowUp') {
          e.preventDefault();
          if (filteredSymbols.length > 0) {
            highlightedIndex = (highlightedIndex - 1 + filteredSymbols.length) % filteredSymbols.length;
            updateHighlight();
          }
        } else if (e.key === 'Enter') {
          e.preventDefault();
          if (highlightedIndex >= 0 && highlightedIndex < filteredSymbols.length) {
            selectSymbol(filteredSymbols[highlightedIndex].symbol);
          } else if (filteredSymbols.length > 0) {
            selectSymbol(filteredSymbols[0].symbol);
          }
        } else if (e.key === 'Escape') {
          closeDropdown();
        }
      });

      document.addEventListener('click', (e) => {
        if (!combobox.contains(e.target)) {
          closeDropdown();
        }
      });
    }

    function renderSymbolList(query) {
      const listEl = document.getElementById('symbol-dropdown-list');
      const badgeEl = document.getElementById('symbol-count-badge');
      const currentSym = document.getElementById('sel-symbol').value;
      const q = (query || '').toLowerCase().trim();

      let searchQ = q;
      if (searchQ.includes(' - ')) {
        searchQ = searchQ.split(' - ')[0].trim();
      }

      if (!searchQ) {
        filteredSymbols = allSymbols;
      } else {
        filteredSymbols = allSymbols
          .map(item => {
            const sym = item.symbol.toLowerCase();
            const name = item.company_name.toLowerCase();
            let score = 0;
            if (sym === searchQ) score = 100;
            else if (sym.startsWith(searchQ)) score = 80;
            else if (sym.includes(searchQ)) score = 60;
            else if (name.startsWith(searchQ)) score = 50;
            else if (name.split(/\s+/).some(w => w.startsWith(searchQ))) score = 45;
            else if (name.includes(searchQ)) score = 25;
            return { item, score };
          })
          .filter(x => x.score > 0)
          .sort((a, b) => b.score - a.score || a.item.symbol.localeCompare(b.item.symbol))
          .map(x => x.item);
      }

      badgeEl.textContent = `${filteredSymbols.length} of ${allSymbols.length} Symbols`;

      if (filteredSymbols.length === 0) {
        listEl.innerHTML = `<div class="symbol-no-match">No symbols matching "<b>${escapeHtml(query)}</b>"</div>`;
        return;
      }

      const visible = filteredSymbols.slice(0, 80);
      listEl.innerHTML = visible.map((item, idx) => {
        const isSelected = item.symbol === currentSym;
        const isHighlighted = idx === highlightedIndex;
        const tagClass = item.cached_count > 0 ? 'local' : 'cloud';
        const tagText = item.cached_count > 0 ? `${item.cached_count}y local` : '☁ Cloud';
        return `
          <div class="symbol-item ${isSelected ? 'selected' : ''} ${isHighlighted ? 'highlighted' : ''}" 
               data-symbol="${item.symbol}" 
               onclick="selectSymbol('${item.symbol}')">
            <div class="symbol-item-left">
              <span class="symbol-ticker">${item.symbol}</span>
              <span class="symbol-name">${escapeHtml(item.company_name)}</span>
            </div>
            <span class="symbol-tag ${tagClass}">${tagText}</span>
          </div>
        `;
      }).join('');
    }

    function updateHighlight() {
      const items = document.querySelectorAll('#symbol-dropdown-list .symbol-item');
      items.forEach((el, idx) => {
        if (idx === highlightedIndex) {
          el.classList.add('highlighted');
          el.scrollIntoView({ block: 'nearest' });
        } else {
          el.classList.remove('highlighted');
        }
      });
    }

    function selectSymbol(symbol) {
      const sel = document.getElementById('sel-symbol');
      const combobox = document.getElementById('symbol-combobox');
      
      sel.value = symbol;
      updateSymbolInputDisplay(symbol);

      combobox.classList.remove('open');
      highlightedIndex = -1;
      onSymbolChange();
    }

    // ==========================================
    // INITIALIZATION & TAB SWITCHING
    // ==========================================
    async function initApp() {
      setStatus('Loading universe symbols...');
      const data = await apiGet('/api/symbols');
      allSymbols = data.symbols || [];
      const sel = document.getElementById('sel-symbol');
      sel.innerHTML = '';
      let defaultSym = 'AAPL';
      if (!allSymbols.some(s => s.symbol === defaultSym) && allSymbols.length > 0) {
        defaultSym = allSymbols[0].symbol;
      }
      for (const item of allSymbols) {
        const opt = document.createElement('option');
        opt.value = item.symbol;
        const cacheTag = item.cached_count > 0 ? `[${item.cached_count}y local]` : `[☁ Cloud]`;
        opt.textContent = `${item.symbol} - ${item.company_name.slice(0, 22)} ${cacheTag}`;
        if (item.symbol === defaultSym) opt.selected = true;
        sel.appendChild(opt);
      }
      setupSymbolCombobox(defaultSym);
      await onSymbolChange();
    }

    async function onSymbolChange() {
      const sym = document.getElementById('sel-symbol').value;
      if (!sym) return;
      updateSymbolInputDisplay(sym);
      setStatus(`Scanning Parquet metadata for ${sym}...`);

      // Invalidate caches so inactive tabs fetch fresh data when switched to
      lifecycleJumpToken++;
      chainLoadToken++;
      seriesLoadToken++;
      smileLoadToken++;
      currentChainData = null;
      currentSeries = [];
      currentContractsMeta = null;
      originChainContext = null;
      closeBanner();

      try {
        const meta = await apiGet(`/api/metadata?symbol=${encodeURIComponent(sym)}`);
        currentMeta = meta;

        // Populate Chain Trade Dates dropdown
        const chainDateSel = document.getElementById('sel-chain-date');
        chainDateSel.innerHTML = '';
        const contractDateSel = document.getElementById('sel-contract-date');
        contractDateSel.innerHTML = '';
        const smileDateSel = document.getElementById('sel-tradedate');
        smileDateSel.innerHTML = '';

        const dates = meta.trade_dates || [];
        for (let i = dates.length - 1; i >= 0; i--) {
          const d = dates[i];
          const o1 = document.createElement('option');
          o1.value = d; o1.textContent = d;
          chainDateSel.appendChild(o1);

          const o2 = document.createElement('option');
          o2.value = d; o2.textContent = d;
          contractDateSel.appendChild(o2);

          const o3 = document.createElement('option');
          o3.value = d; o3.textContent = d;
          smileDateSel.appendChild(o3);
        }

        // Populate Years dropdown
        const yearSel = document.getElementById('sel-year');
        yearSel.innerHTML = '';
        const years = meta.available_years || [];
        const latestYr = years.length > 0 ? years[years.length - 1] : 2026;
        for (const yr of [...years].reverse()) {
          const opt = document.createElement('option');
          opt.value = yr; opt.textContent = yr;
          if (yr === latestYr) opt.selected = true;
          yearSel.appendChild(opt);
        }
        const allOpt = document.createElement('option');
        allOpt.value = ''; allOpt.textContent = 'All Years (2016-2026)';
        yearSel.appendChild(allOpt);

        document.getElementById('cache-badge').textContent = `${sym}: ${years.length} Yearly Parquets Cached`;

        if (currentTab === 'chain') {
          await loadOptionChain();
        } else if (currentTab === 'smile') {
          await loadChainSmile();
        } else {
          await onYearChange();
        }
      } catch (e) {
        setStatus(`Error loading ${sym}: ${e.message}`);
      }
    }

    function switchTab(tab, skipAutoLoad = false) {
      currentTab = tab;

      // Update Tab Buttons
      document.getElementById('tab-btn-chain').classList.toggle('active', tab === 'chain');
      document.getElementById('tab-btn-lifecycle').classList.toggle('active', tab === 'lifecycle');
      document.getElementById('tab-btn-smile').classList.toggle('active', tab === 'smile');
      document.getElementById('tab-btn-table').classList.toggle('active', tab === 'table');

      // Update View Containers
      document.getElementById('view-chain').classList.toggle('hidden', tab !== 'chain');
      document.getElementById('view-lifecycle').classList.toggle('hidden', tab !== 'lifecycle');
      document.getElementById('view-smile').classList.toggle('hidden', tab !== 'smile');
      document.getElementById('view-table').classList.toggle('hidden', tab !== 'table');

      // Update Toolbar Fields Visibility
      const isChain = (tab === 'chain');
      const isLifecycle = (tab === 'lifecycle');
      const isSmile = (tab === 'smile');
      const isTable = (tab === 'table');

      document.getElementById('fld-chain-date').classList.toggle('hidden', !isChain);
      document.getElementById('fld-chain-exp').classList.toggle('hidden', !isChain);
      document.getElementById('fld-strike-range').classList.toggle('hidden', !isChain);
      document.getElementById('fld-strike-search').classList.toggle('hidden', !isChain);

      document.getElementById('fld-year').classList.toggle('hidden', isChain || isSmile);
      document.getElementById('fld-expiration').classList.toggle('hidden', isChain);
      document.getElementById('fld-right').classList.toggle('hidden', isChain || isSmile);
      document.getElementById('fld-strike').classList.toggle('hidden', isChain || isSmile);
      document.getElementById('fld-quick').classList.toggle('hidden', isChain || isSmile);
      document.getElementById('fld-jump-chain').classList.toggle('hidden', !isLifecycle);

      document.getElementById('fld-tradedate').classList.toggle('hidden', !isSmile);

      // Trigger Resize on active plots
      requestAnimationFrame(() => {
        const container = document.getElementById(tab === 'lifecycle' ? 'view-lifecycle' : (tab === 'smile' ? 'view-smile' : 'view-chain'));
        if (container) {
          container.querySelectorAll('.chart-box').forEach(box => {
            if (box._fullLayout) {
              Plotly.Plots.resize(box);
            }
          });
        }
      });

      if (!skipAutoLoad) {
        if (tab === 'chain') {
          if (!currentChainData) loadOptionChain();
        } else if (tab === 'lifecycle' || tab === 'table') {
          if (!currentSeries.length) onYearChange();
        } else if (tab === 'smile') {
          loadChainSmile();
        }
      }
    }

    // =========================================================
    // TAB 1: DAILY OPTION CHAIN (SNAPSHOT BY DATE) LOGIC
    // =========================================================
    let chainLoadToken = 0;
    async function loadOptionChain(requestedExp = null, targetStrike = null) {
      const thisToken = ++chainLoadToken;
      const sym = document.getElementById('sel-symbol').value;
      const date = document.getElementById('sel-chain-date').value;
      if (!sym) return;

      setStatus(`Loading daily option chain for ${sym} on ${date || 'latest date'}...`);
      let url = `/api/chain?symbol=${encodeURIComponent(sym)}`;
      if (date) url += `&trade_date=${encodeURIComponent(date)}`;
      if (requestedExp) url += `&expiration=${encodeURIComponent(requestedExp)}`;

      try {
        const data = await apiGet(url);
        if (thisToken !== chainLoadToken) return;
        currentChainData = data;

        // Keep dropdown in sync
        if (data.trade_date) {
          document.getElementById('sel-chain-date').value = data.trade_date;
          document.getElementById('chain-exp-date-label').textContent = data.trade_date;
        }
        document.getElementById('chain-exp-sym-label').textContent = sym;

        // Update Snapshot KPIs
        const spot = data.underlying_price;
        document.getElementById('chain-kpi-spot').textContent = spot ? `$${spot.toFixed(2)}` : '--';
        document.getElementById('th-strike-title').textContent = spot ? `STRIKE (Spot: $${spot.toFixed(2)})` : 'STRIKE & SPOT';

        const exps = data.expirations || [];
        document.getElementById('chain-kpi-exps-count').textContent = `${exps.length} Expirations`;
        if (exps.length > 0) {
          const dtes = exps.map(e => e.dte).filter(d => d != null);
          const minDte = Math.min(...dtes);
          const maxDte = Math.max(...dtes);
          document.getElementById('chain-kpi-dte-range').textContent = `DTE: ${minDte}d to ${maxDte}d`;
        } else {
          document.getElementById('chain-kpi-dte-range').textContent = '--';
        }

        const selExp = data.selected_expiration || '--';
        const selDte = data.selected_dte != null ? `${data.selected_dte}d DTE` : '';
        document.getElementById('chain-kpi-selected-exp').textContent = `${selExp} (${selDte})`;
        document.getElementById('chain-kpi-strikes-count').textContent = `${(data.strikes || []).length} strikes`;

        const atmRow = (data.strikes || []).find(s => s.is_atm);
        document.getElementById('chain-kpi-atm').textContent = atmRow ? `ATM Strike: $${atmRow.strike}` : 'ATM Strike: --';

        const totals = data.totals || {};
        const sessionTotVol = totals.session_total_volume ?? totals.total_volume ?? 0;
        const sessionCallVol = totals.session_call_volume ?? totals.call_volume ?? 0;
        const sessionPutVol = totals.session_put_volume ?? totals.put_volume ?? 0;
        const pcVol = totals.session_put_call_volume_ratio ?? totals.put_call_volume_ratio;

        const sessionTotOI = totals.session_total_oi ?? totals.total_oi ?? 0;
        const sessionCallOI = totals.session_call_oi ?? totals.call_oi ?? 0;
        const sessionPutOI = totals.session_put_oi ?? totals.put_oi ?? 0;
        const pcOI = totals.session_put_call_oi_ratio ?? totals.put_call_oi_ratio;

        document.getElementById('chain-kpi-vol').textContent = sessionTotVol.toLocaleString();
        document.getElementById('chain-kpi-pc-vol').textContent = pcVol != null
          ? `P/C Vol: ${pcVol.toFixed(2)} (Calls: ${sessionCallVol.toLocaleString()} • Puts: ${sessionPutVol.toLocaleString()})`
          : 'P/C Vol: --';

        document.getElementById('chain-kpi-oi').textContent = sessionTotOI.toLocaleString();
        document.getElementById('chain-kpi-pc-oi').textContent = pcOI != null
          ? `P/C OI: ${pcOI.toFixed(2)} (Calls: ${sessionCallOI.toLocaleString()} • Puts: ${sessionPutOI.toLocaleString()})`
          : 'P/C OI: --';

        // Update Chain Expiration Dropdown & Pills Bar
        renderChainExpirations(exps, data.selected_expiration);

        // Render Table Rows
        filterChainStrikes(targetStrike);

        setStatus(`Loaded option chain for ${sym} on ${data.trade_date} (${exps.length} expirations, ${(data.strikes || []).length} strikes)`);
      } catch (e) {
        if (thisToken !== chainLoadToken) return;
        setStatus(`Error loading chain: ${e.message}`);
      }
    }

    function renderChainExpirations(exps, selectedExp) {
      const selectEl = document.getElementById('sel-chain-exp');
      selectEl.innerHTML = '';

      const pillsContainer = document.getElementById('exp-pills-bar');
      pillsContainer.innerHTML = '';

      exps.forEach(e => {
        const isSelected = e.expiration === selectedExp;
        // Option
        const opt = document.createElement('option');
        opt.value = e.expiration;
        opt.textContent = `${e.expiration} (${e.dte ?? '--'}d) - ${e.contract_count} contracts`;
        if (isSelected) opt.selected = true;
        selectEl.appendChild(opt);

        // Pill
        const pill = document.createElement('div');
        pill.className = `exp-pill ${isSelected ? 'active' : ''}`;
        pill.title = `${e.contract_count} contracts • Vol: ${(e.total_volume || 0).toLocaleString()} • OI: ${(e.total_oi || 0).toLocaleString()}`;
        pill.innerHTML = `
          <span>${e.expiration}</span>
          <span class="dte-tag">${e.dte ?? '--'}d</span>
          <span class="vol-tag">${(e.total_volume || 0) > 1000 ? Math.round(e.total_volume / 1000) + 'k v' : (e.total_volume || 0) + ' v'}</span>
        `;
        pill.onclick = () => selectChainExp(e.expiration);
        pillsContainer.appendChild(pill);
      });
    }

    function selectChainExp(expiration) {
      document.getElementById('sel-chain-exp').value = expiration;
      loadOptionChain(expiration);
    }

    function onChainExpSelectChange() {
      const exp = document.getElementById('sel-chain-exp').value;
      loadOptionChain(exp);
    }

    function onChainDateChange() {
      loadOptionChain();
    }

    function stepTradeDate(direction) {
      const sel = document.getElementById('sel-chain-date');
      const idx = sel.selectedIndex;
      // Note: options are ordered newest -> oldest (idx 0 is newest)
      // Step -1 means previous day (older, so index + 1)
      // Step +1 means next day (newer, so index - 1)
      const targetIdx = idx - direction;
      if (targetIdx >= 0 && targetIdx < sel.options.length) {
        sel.selectedIndex = targetIdx;
        loadOptionChain();
      }
    }

    function jumpToLatestDate() {
      const sel = document.getElementById('sel-chain-date');
      if (sel.options.length > 0) {
        sel.selectedIndex = 0;
        loadOptionChain();
      }
    }

    function filterChainStrikes(targetStrike = null) {
      if (!currentChainData || !currentChainData.strikes) return;
      const strikes = currentChainData.strikes;
      const spot = currentChainData.underlying_price || 0;
      const rangeVal = document.getElementById('sel-strike-range').value;
      const searchVal = document.getElementById('input-strike-search').value.trim();

      let filtered = strikes;

      // Range Filter
      if (rangeVal !== 'all' && spot > 0) {
        const pct = parseFloat(rangeVal) / 100.0;
        const minSt = spot * (1.0 - pct);
        const maxSt = spot * (1.0 + pct);
        filtered = filtered.filter(s => s.strike >= minSt && s.strike <= maxSt);
      }

      // Quick Search Filter
      if (searchVal) {
        const num = parseFloat(searchVal);
        if (!isNaN(num)) {
          filtered = filtered.filter(s => String(s.strike).includes(searchVal) || Math.abs(s.strike - num) <= 5.0);
        }
      }

      renderChainTable(filtered, targetStrike);
    }

    function renderChainTable(strikeRows, targetStrike = null) {
      const tbody = document.getElementById('chain-table-body');
      tbody.innerHTML = '';

      if (!strikeRows.length) {
        tbody.innerHTML = `<tr><td colspan="16" style="text-align:center; padding:24px; color:var(--text-muted);">No strikes matched the current filter. Try selecting "All Available Strikes".</td></tr>`;
        return;
      }

      const sym = currentChainData.symbol;
      const exp = currentChainData.selected_expiration;
      const tradeDate = currentChainData.trade_date;
      const spot = currentChainData.underlying_price || 0;

      const fmt = (v, d = 2) => (v != null ? Number(v).toFixed(d) : '--');
      const fmtVol = v => (v != null && v > 0 ? Number(v).toLocaleString() : '-');

      for (const row of strikeRows) {
        const st = row.strike;
        const isATM = row.is_atm;
        const isTarget = targetStrike !== null && Math.abs(st - targetStrike) < 1e-4;

        const c = row.call;
        const p = row.put;

        const isCallITM = spot > 0 && st < spot;
        const isPutITM = spot > 0 && st > spot;

        const tr = document.createElement('tr');
        tr.id = `chain-strike-${String(st).replace('.', '_')}`;
        let rowClass = '';
        if (isCallITM) rowClass += ' itm-call';
        if (isPutITM) rowClass += ' itm-put';
        if (isTarget) rowClass += ' highlight-pulse';
        tr.className = rowClass.trim();

        // Moneyness %
        const moneynessText = row.moneyness_pct != null 
          ? (row.moneyness_pct >= 0 ? `+${row.moneyness_pct}%` : `${row.moneyness_pct}%`) 
          : '--';

        tr.innerHTML = `
          <!-- CALLS SIDE -->
          <td style="text-align:center;">
            <button class="btn-action-view" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'call', '${tradeDate}')" title="Analyze ${sym} ${exp} $${st} CALL Lifecycle">
              📈 View
            </button>
          </td>
          <td class="call-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'call', '${tradeDate}')" style="color:var(--accent); font-weight:500;">
            ${c ? fmt(c.delta, 2) : '--'}
          </td>
          <td class="call-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'call', '${tradeDate}')" style="color:var(--accent-amber);">
            ${c && c.implied_vol != null ? (c.implied_vol * 100).toFixed(1) + '%' : '--'}
          </td>
          <td class="call-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'call', '${tradeDate}')" style="color:var(--text-muted); font-size:11px;">
            ${c ? `$${fmt(c.bid)} / $${fmt(c.ask)}` : '--'}
          </td>
          <td class="call-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'call', '${tradeDate}')" style="color:#fff; font-weight:700;">
            ${c ? `$${fmt(c.mid ?? c.close)}` : '--'}
          </td>
          <td class="call-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'call', '${tradeDate}')">
            ${c ? fmtVol(c.volume) : '-'}
          </td>
          <td class="call-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'call', '${tradeDate}')" style="border-right:1px solid #334155;">
            ${c ? fmtVol(c.open_interest) : '-'}
          </td>

          <!-- CENTER STRIKE -->
          <td class="td-strike ${isATM ? 'strike-atm' : ''}">
            ${st}${isATM ? '<span class="atm-badge">ATM</span>' : ''}
          </td>
          <td style="background:#141e30; color:var(--text-muted); font-size:10.5px; text-align:center; border-right:1px solid #334155;">
            ${moneynessText}
          </td>

          <!-- PUTS SIDE -->
          <td class="put-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'put', '${tradeDate}')" style="color:#fff; font-weight:700;">
            ${p ? `$${fmt(p.mid ?? p.close)}` : '--'}
          </td>
          <td class="put-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'put', '${tradeDate}')" style="color:var(--text-muted); font-size:11px;">
            ${p ? `$${fmt(p.bid)} / $${fmt(p.ask)}` : '--'}
          </td>
          <td class="put-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'put', '${tradeDate}')" style="color:var(--accent-amber);">
            ${p && p.implied_vol != null ? (p.implied_vol * 100).toFixed(1) + '%' : '--'}
          </td>
          <td class="put-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'put', '${tradeDate}')" style="color:var(--accent-red); font-weight:500;">
            ${p ? fmt(p.delta, 2) : '--'}
          </td>
          <td class="put-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'put', '${tradeDate}')">
            ${p ? fmtVol(p.volume) : '-'}
          </td>
          <td class="put-cell clickable" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'put', '${tradeDate}')">
            ${p ? fmtVol(p.open_interest) : '-'}
          </td>
          <td style="text-align:center;">
            <button class="btn-action-view" onclick="jumpToLifecycle('${sym}', '${exp}', ${st}, 'put', '${tradeDate}')" title="Analyze ${sym} ${exp} $${st} PUT Lifecycle">
              📈 View
            </button>
          </td>
        `;

        tbody.appendChild(tr);
      }

      if (targetStrike !== null) {
        const targetRow = document.getElementById(`chain-strike-${String(targetStrike).replace('.', '_')}`);
        if (targetRow) {
          targetRow.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
      }
    }

    // =========================================================
    // BIDIRECTIONAL NAVIGATION & JUMP FUNCTIONS
    // =========================================================
    let lifecycleJumpToken = 0;

    async function jumpToLifecycle(symbol, expiration, strike, right, originTradeDate) {
      const thisToken = ++lifecycleJumpToken;
      originChainContext = {
        symbol: symbol,
        tradeDate: originTradeDate,
        expiration: expiration
      };

      // Set banner info
      showBanner(
        `Viewing ${symbol} ${expiration} $${strike} ${right.toUpperCase()}`,
        `Selected from ${originTradeDate} Option Chain Snapshot`,
        `⬅ Back to ${originTradeDate} Option Chain`,
        returnToChain
      );

      // Switch tab to Contract Lifecycle without triggering competing auto-load
      switchTab('lifecycle', true);

      // Ensure symbol selection matches
      const symSel = document.getElementById('sel-symbol');
      if (symSel.value !== symbol) {
        symSel.value = symbol;
        updateSymbolInputDisplay(symbol);
      }

      // Set Year filter to All Years ('') so the full multi-year contract lifecycle is queried
      const yearSel = document.getElementById('sel-year');
      if (![...yearSel.options].some(o => o.value === '')) {
        const opt = document.createElement('option');
        opt.value = ''; opt.textContent = 'All Years';
        yearSel.appendChild(opt);
      }
      yearSel.value = '';

      // Populate Expiration dropdown with target expiration
      const expSel = document.getElementById('sel-expiration');
      if (![...expSel.options].some(o => o.value === expiration)) {
        const opt = document.createElement('option');
        opt.value = expiration;
        opt.textContent = expiration;
        expSel.appendChild(opt);
      }
      expSel.value = expiration;

      // Populate Strike dropdown with target strike
      const strikeSel = document.getElementById('sel-strike');
      const numStrike = parseFloat(strike);
      if (![...strikeSel.options].some(o => Math.abs(parseFloat(o.value) - numStrike) < 1e-4)) {
        const opt = document.createElement('option');
        opt.value = numStrike;
        opt.textContent = `${numStrike}`;
        strikeSel.appendChild(opt);
      }
      strikeSel.value = numStrike;

      // Update Option Type
      document.getElementById('sel-right').value = right.toLowerCase();

      // Sync Contract Date dropdown
      const contractDateSel = document.getElementById('sel-contract-date');
      if (contractDateSel && originTradeDate) {
        contractDateSel.value = originTradeDate;
      }

      // Load timeseries directly for this specific contract
      await loadContractSeries();

      // Hydrate sibling expirations and strikes across all years in background if user hasn't jumped or changed selection
      apiGet(`/api/metadata?symbol=${encodeURIComponent(symbol)}`).then(meta => {
        if (thisToken !== lifecycleJumpToken) return;
        if (document.getElementById('sel-symbol').value !== symbol) return;
        currentMeta = meta;
        const curExp = expSel.value;
        expSel.innerHTML = '';
        (meta.expirations || []).forEach(ex => {
          const opt = document.createElement('option');
          opt.value = ex; opt.textContent = ex;
          if (ex === curExp) opt.selected = true;
          expSel.appendChild(opt);
        });
        if (![...expSel.options].some(o => o.value === curExp)) {
          const opt = document.createElement('option');
          opt.value = curExp; opt.textContent = curExp; opt.selected = true;
          expSel.appendChild(opt);
        }
      }).catch(() => {});

      apiGet(`/api/contracts?symbol=${encodeURIComponent(symbol)}&expiration=${encodeURIComponent(expiration)}`).then(data => {
        if (thisToken !== lifecycleJumpToken) return;
        if (document.getElementById('sel-symbol').value !== symbol || expSel.value !== expiration) return;
        currentContractsMeta = data;
        const curStrike = strikeSel.value;
        strikeSel.innerHTML = '';
        const atm = data.atm_strike;
        (data.strikes || []).forEach(st => {
          const opt = document.createElement('option');
          opt.value = st;
          const isAtm = atm !== null && Math.abs(st - atm) < 1e-4;
          opt.textContent = isAtm ? `${st} ★ (ATM)` : `${st}`;
          if (Math.abs(st - parseFloat(curStrike)) < 1e-4) opt.selected = true;
          strikeSel.appendChild(opt);
        });
        if (![...strikeSel.options].some(o => Math.abs(parseFloat(o.value) - parseFloat(curStrike)) < 1e-4)) {
          const opt = document.createElement('option');
          opt.value = curStrike; opt.textContent = curStrike; opt.selected = true;
          strikeSel.appendChild(opt);
        }
      }).catch(() => {});
    }

    function returnToChain() {
      if (!originChainContext) return;
      const { symbol, tradeDate, expiration } = originChainContext;
      const targetStrike = document.getElementById('sel-strike').value;

      document.getElementById('sel-chain-date').value = tradeDate;
      switchTab('chain', true);
      loadOptionChain(expiration, parseFloat(targetStrike));
    }

    function jumpFromContractToChain(targetDate = null) {
      const sym = document.getElementById('sel-symbol').value;
      const date = targetDate || document.getElementById('sel-contract-date').value;
      const exp = document.getElementById('sel-expiration').value;
      const strike = parseFloat(document.getElementById('sel-strike').value);

      document.getElementById('sel-chain-date').value = date;
      switchTab('chain', true);
      loadOptionChain(exp, strike);
    }

    function showBanner(title, subtitle, actionText, actionFn) {
      const banner = document.getElementById('nav-context-banner');
      document.getElementById('banner-text').textContent = title;
      document.getElementById('banner-sub').textContent = subtitle;
      const actionBtn = document.getElementById('btn-banner-action');
      actionBtn.textContent = actionText;
      actionBtn.onclick = actionFn;
      banner.classList.remove('hidden');
    }

    function closeBanner() {
      document.getElementById('nav-context-banner').classList.add('hidden');
    }

    function bannerActionClick() {
      if (originChainContext) {
        returnToChain();
      }
    }

    // =========================================================
    // TAB 2: CONTRACT LIFECYCLE (TIME SERIES) LOGIC
    // =========================================================
    async function onYearChange(autoLoad = true) {
      lifecycleJumpToken++;
      const sym = document.getElementById('sel-symbol').value;
      const yr = document.getElementById('sel-year').value;
      setStatus(`Loading expirations for ${sym} ${yr || 'All'}...`);
      const url = `/api/metadata?symbol=${encodeURIComponent(sym)}` + (yr ? `&year=${yr}` : '');
      const meta = await apiGet(url);
      currentMeta = meta;

      const expSel = document.getElementById('sel-expiration');
      expSel.innerHTML = '';
      const exps = meta.expirations || [];
      const defaultIdx = Math.max(0, Math.min(exps.length - 1, Math.floor(exps.length * 0.7)));
      exps.forEach((ex, i) => {
        const opt = document.createElement('option');
        opt.value = ex; opt.textContent = ex;
        if (i === defaultIdx) opt.selected = true;
        expSel.appendChild(opt);
      });

      if (autoLoad) {
        if (currentTab === 'smile') {
          await loadChainSmile();
        } else {
          await onExpirationChange();
        }
      }
    }

    async function onExpirationChange(autoLoad = true) {
      lifecycleJumpToken++;
      const sym = document.getElementById('sel-symbol').value;
      const yr = document.getElementById('sel-year').value;
      const exp = document.getElementById('sel-expiration').value;
      if (!sym || !exp) return;

      setStatus(`Fetching strikes for ${sym} exp ${exp}...`);
      const url = `/api/contracts?symbol=${encodeURIComponent(sym)}&expiration=${encodeURIComponent(exp)}` + (yr ? `&year=${yr}` : '');
      const data = await apiGet(url);
      currentContractsMeta = data;

      const strikeSel = document.getElementById('sel-strike');
      strikeSel.innerHTML = '';
      const strikes = data.strikes || [];
      const atm = data.atm_strike;

      for (const st of strikes) {
        const opt = document.createElement('option');
        opt.value = st;
        const isAtm = atm !== null && Math.abs(st - atm) < 1e-4;
        opt.textContent = isAtm ? `${st} ★ (ATM)` : `${st}`;
        if (isAtm) opt.selected = true;
        strikeSel.appendChild(opt);
      }

      if (autoLoad) {
        await loadContractSeries();
      }
    }

    function pickATM() {
      if (!currentContractsMeta || currentContractsMeta.atm_strike === null) return;
      document.getElementById('sel-strike').value = currentContractsMeta.atm_strike;
      loadContractSeries();
    }

    function pickMaxOI() {
      if (!currentContractsMeta || !currentContractsMeta.contracts) return;
      const right = document.getElementById('sel-right').value;
      const filtered = currentContractsMeta.contracts.filter(c => c.right === right);
      if (!filtered.length) return;
      filtered.sort((a, b) => (b.max_oi || 0) - (a.max_oi || 0));
      document.getElementById('sel-strike').value = filtered[0].strike;
      loadContractSeries();
    }

    let seriesLoadToken = 0;
    async function loadContractSeries() {
      const thisToken = ++seriesLoadToken;
      const sym = document.getElementById('sel-symbol').value;
      const yr = document.getElementById('sel-year').value;
      const exp = document.getElementById('sel-expiration').value;
      const strike = document.getElementById('sel-strike').value;
      const right = document.getElementById('sel-right').value;
      if (!sym || !exp || !strike) return;

      setStatus(`Querying ${sym} ${exp} $${strike} ${right.toUpperCase()}...`);
      const url = `/api/timeseries?symbol=${encodeURIComponent(sym)}&expiration=${encodeURIComponent(exp)}&strike=${encodeURIComponent(strike)}&right=${encodeURIComponent(right)}` + (yr ? `&year=${yr}` : '');
      try {
        const data = await apiGet(url);
        if (thisToken !== seriesLoadToken) return;
        currentSeries = data.series || [];
        renderLifecycleCharts(data);
        renderTable(currentSeries);
        setStatus(`Loaded ${currentSeries.length} trading days for ${sym} ${exp} $${strike} ${right.toUpperCase()}`);
      } catch (e) {
        if (thisToken !== seriesLoadToken) return;
        setStatus(`Error querying contract series: ${e.message}`);
      }
    }

    function renderLifecycleCharts(data) {
      const rows = data.series || [];
      if (!rows.length) {
        setStatus('No rows found for this contract.');
        return;
      }
      const last = rows[rows.length - 1];
      const strikeNum = Number(data.strike);

      // Update KPI cards
      document.getElementById('kpi-contract').textContent = `${data.symbol} $${data.strike} ${data.right.toUpperCase()}`;
      document.getElementById('kpi-days').textContent = `Exp: ${data.expiration} (${rows.length} days)`;
      document.getElementById('kpi-price').textContent = `$${(last.mid ?? last.close ?? 0).toFixed(2)}`;
      document.getElementById('kpi-bidask').textContent = `Bid $${(last.bid ?? 0).toFixed(2)} / Ask $${(last.ask ?? 0).toFixed(2)}`;
      document.getElementById('kpi-spot').textContent = `$${(last.underlying_price ?? 0).toFixed(2)}`;
      const moneyness = last.underlying_price ? (((last.underlying_price - strikeNum) / strikeNum) * 100).toFixed(1) : '--';
      document.getElementById('kpi-moneyness').textContent = `vs Strike: ${moneyness}%`;
      document.getElementById('kpi-iv').textContent = last.implied_vol != null ? `${(last.implied_vol * 100).toFixed(1)}%` : '--';
      document.getElementById('kpi-dte').textContent = `Latest DTE: ${last.dte ?? '--'}d`;
      document.getElementById('kpi-delta').textContent = last.delta != null ? last.delta.toFixed(3) : '--';
      document.getElementById('kpi-gamma').textContent = `Gamma: ${last.gamma != null ? last.gamma.toFixed(4) : '--'}`;
      document.getElementById('kpi-theta').textContent = last.theta != null ? last.theta.toFixed(3) : '--';
      document.getElementById('kpi-vega').textContent = `Vega/1%: ${last.vega_per_pct != null ? last.vega_per_pct.toFixed(3) : '--'}`;
      document.getElementById('kpi-oi').textContent = (last.open_interest ?? 0).toLocaleString();
      const maxVol = Math.max(...rows.map(r => r.volume || 0));
      document.getElementById('kpi-vol').textContent = `Peak Vol: ${maxVol.toLocaleString()}`;

      const dates = rows.map(r => r.trade_date);

      // Chart 1: Option Mid/Bid/Ask Price vs Underlying Price
      renderPlot('chart-price', [
        {
          x: dates, y: rows.map(r => r.ask),
          name: 'Option Ask', mode: 'lines',
          line: { color: 'rgba(56, 189, 248, 0.25)', width: 1 }
        },
        {
          x: dates, y: rows.map(r => r.bid),
          name: 'Option Bid', mode: 'lines', fill: 'tonexty',
          fillcolor: 'rgba(56, 189, 248, 0.1)',
          line: { color: 'rgba(56, 189, 248, 0.25)', width: 1 }
        },
        {
          x: dates, y: rows.map(r => r.mid ?? r.close),
          name: 'Option Mid ($)', mode: 'lines+markers',
          marker: { size: 4 },
          line: { color: '#38bdf8', width: 2.5 }
        },
        {
          x: dates, y: rows.map(r => r.underlying_price),
          name: `${data.symbol} Underlying Spot ($)`, yaxis: 'y2', mode: 'lines',
          line: { color: '#f59e0b', width: 2, dash: 'dot' }
        },
        {
          x: [dates[0], dates[dates.length - 1]], y: [strikeNum, strikeNum],
          name: `Strike ($${strikeNum})`, yaxis: 'y2', mode: 'lines',
          line: { color: '#ef4444', width: 1.2, dash: 'dash' }
        }
      ], {
        title: { text: `${data.symbol} ${data.expiration} $${data.strike} ${data.right.toUpperCase()} — Option Price vs Underlying Spot`, font: { size: 13 } },
        yaxis: { title: 'Option Price ($)' },
        yaxis2: {
          title: 'Underlying Price ($)',
          overlaying: 'y',
          side: 'right',
          gridcolor: 'transparent'
        }
      });

      // Chart 2: Delta & Gamma
      renderPlot('chart-greeks-1', [
        {
          x: dates, y: rows.map(r => r.delta),
          name: 'Delta (Δ)', mode: 'lines',
          line: { color: '#38bdf8', width: 2 }
        },
        {
          x: dates, y: rows.map(r => r.gamma),
          name: 'Gamma (Γ)', yaxis: 'y2', mode: 'lines',
          line: { color: '#a855f7', width: 2 }
        }
      ], {
        title: { text: 'Directional & Convexity Greeks: Delta (Δ) & Gamma (Γ)', font: { size: 13 } },
        yaxis: { title: 'Delta' },
        yaxis2: { title: 'Gamma', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      });

      // Chart 3: Theta, Vega_per_pct, Rho_per_pct
      renderPlot('chart-greeks-2', [
        {
          x: dates, y: rows.map(r => r.theta),
          name: 'Theta (Θ / day)', mode: 'lines',
          line: { color: '#ef4444', width: 2 }
        },
        {
          x: dates, y: rows.map(r => r.vega_per_pct ?? r.vega),
          name: 'Vega (per 1% IV)', yaxis: 'y2', mode: 'lines',
          line: { color: '#22c55e', width: 2 }
        },
        {
          x: dates, y: rows.map(r => r.rho_per_pct ?? r.rho),
          name: 'Rho (per 1% Rate)', yaxis: 'y2', mode: 'lines',
          line: { color: '#94a3b8', width: 1.5, dash: 'dot' }
        }
      ], {
        title: { text: 'Time Decay & Volatility Sensitivity: Theta (Θ), Vega & Rho', font: { size: 13 } },
        yaxis: { title: 'Theta ($/day)' },
        yaxis2: { title: 'Vega / Rho ($ per 1%)', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      });

      // Chart 4: Implied Volatility + Open Interest & Volume
      renderPlot('chart-iv-oi', [
        {
          x: dates, y: rows.map(r => r.open_interest),
          name: 'Open Interest', type: 'bar',
          marker: { color: 'rgba(34, 197, 94, 0.35)' },
          yaxis: 'y2'
        },
        {
          x: dates, y: rows.map(r => r.volume),
          name: 'Daily Volume', type: 'bar',
          marker: { color: 'rgba(56, 189, 248, 0.45)' },
          yaxis: 'y2'
        },
        {
          x: dates, y: rows.map(r => r.implied_vol != null ? r.implied_vol * 100 : null),
          name: 'Implied Volatility (%)', mode: 'lines',
          line: { color: '#f59e0b', width: 2.2 }
        }
      ], {
        barmode: 'overlay',
        title: { text: 'Implied Volatility (%) & Open Interest / Volume', font: { size: 13 } },
        yaxis: { title: 'Implied Vol (%)' },
        yaxis2: { title: 'Contracts (OI / Volume)', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      });

      // Chart 5: Higher-Order Greeks (Vanna, Charm, Vomma)
      renderPlot('chart-higher-greeks', [
        {
          x: dates, y: rows.map(r => r.vanna),
          name: 'Vanna (dΔ/dσ)', mode: 'lines',
          line: { color: '#38bdf8', width: 1.8 }
        },
        {
          x: dates, y: rows.map(r => r.charm),
          name: 'Charm (dΔ/dt)', mode: 'lines',
          line: { color: '#f43f5e', width: 1.8 }
        },
        {
          x: dates, y: rows.map(r => r.vomma),
          name: 'Vomma (dVega/dσ)', yaxis: 'y2', mode: 'lines',
          line: { color: '#a855f7', width: 1.8 }
        }
      ], {
        title: { text: 'Second-Order Greeks: Vanna, Charm & Vomma', font: { size: 13 } },
        yaxis: { title: 'Vanna / Charm' },
        yaxis2: { title: 'Vomma', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      });
    }

    // =========================================================
    // TAB 3: IV SMILE & STRIKE CROSS-SECTION LOGIC
    // =========================================================
    let smileLoadToken = 0;
    async function loadChainSmile() {
      const thisToken = ++smileLoadToken;
      const sym = document.getElementById('sel-symbol').value;
      const td = document.getElementById('sel-tradedate').value;
      const exp = document.getElementById('sel-expiration').value;
      if (!sym || !td) return;

      setStatus(`Loading option chain cross-section for ${sym} on ${td}...`);
      const url = `/api/smile?symbol=${encodeURIComponent(sym)}&trade_date=${encodeURIComponent(td)}&expiration=${encodeURIComponent(exp)}`;
      try {
        const data = await apiGet(url);
        if (thisToken !== smileLoadToken) return;

      const availExps = data.available_expirations || [];
      const expSel = document.getElementById('sel-expiration');
      if (availExps.length && !availExps.includes(exp)) {
        expSel.innerHTML = '';
        availExps.forEach(e => {
          const opt = document.createElement('option');
          opt.value = e; opt.textContent = e;
          if (e === data.selected_expiration) opt.selected = true;
          expSel.appendChild(opt);
        });
      }

      const chain = data.chain || [];
      const calls = chain.filter(r => r.right === 'call');
      const puts = chain.filter(r => r.right === 'put');
      const spot = data.underlying_price;

      const spotShapes = spot ? [{
        type: 'line', x0: spot, x1: spot, y0: 0, y1: 1, yref: 'paper',
        line: { color: '#f59e0b', width: 1.5, dash: 'dash' }
      }] : [];

      renderPlot('chart-smile-iv', [
        {
          x: calls.map(r => r.strike),
          y: calls.map(r => r.implied_vol != null ? r.implied_vol * 100 : null),
          name: 'Call IV (%)', mode: 'lines+markers',
          line: { color: '#22c55e', width: 2 }
        },
        {
          x: puts.map(r => r.strike),
          y: puts.map(r => r.implied_vol != null ? r.implied_vol * 100 : null),
          name: 'Put IV (%)', mode: 'lines+markers',
          line: { color: '#ef4444', width: 2 }
        }
      ], {
        shapes: spotShapes,
        title: { text: `Volatility Smile (${sym} on ${td}, Exp ${data.selected_expiration}, Spot $${spot ?? '--'})`, font: { size: 13 } },
        xaxis: { title: 'Strike Price ($)' },
        yaxis: { title: 'Implied Volatility (%)' }
      });

      renderPlot('chart-smile-delta', [
        {
          x: calls.map(r => r.strike), y: calls.map(r => r.delta),
          name: 'Call Delta', mode: 'lines', line: { color: '#22c55e', width: 2 }
        },
        {
          x: puts.map(r => r.strike), y: puts.map(r => r.delta),
          name: 'Put Delta', mode: 'lines', line: { color: '#ef4444', width: 2 }
        },
        {
          x: calls.map(r => r.strike), y: calls.map(r => r.gamma),
          name: 'Gamma (Γ)', yaxis: 'y2', mode: 'lines', line: { color: '#a855f7', width: 2, dash: 'dot' }
        }
      ], {
        shapes: spotShapes,
        title: { text: 'Delta & Gamma Profile by Strike', font: { size: 13 } },
        xaxis: { title: 'Strike Price ($)' },
        yaxis: { title: 'Delta' },
        yaxis2: { title: 'Gamma', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      });

      renderPlot('chart-smile-gex', [
        {
          x: calls.map(r => r.strike), y: calls.map(r => r.vega_per_pct ?? r.vega),
          name: 'Vega/1%', mode: 'lines', line: { color: '#38bdf8', width: 2 }
        },
        {
          x: calls.map(r => r.strike), y: calls.map(r => r.theta),
          name: 'Call Theta', yaxis: 'y2', mode: 'lines', line: { color: '#f43f5e', width: 1.8 }
        },
        {
          x: puts.map(r => r.strike), y: puts.map(r => r.theta),
          name: 'Put Theta', yaxis: 'y2', mode: 'lines', line: { color: '#fb923c', width: 1.8, dash: 'dot' }
        }
      ], {
        shapes: spotShapes,
        title: { text: 'Vega & Theta Decay Across Strikes', font: { size: 13 } },
        xaxis: { title: 'Strike Price ($)' },
        yaxis: { title: 'Vega ($/1%)' },
        yaxis2: { title: 'Theta ($/day)', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      });

      renderPlot('chart-smile-oi', [
        {
          x: calls.map(r => r.strike), y: calls.map(r => r.open_interest),
          name: 'Call Open Interest', type: 'bar', marker: { color: 'rgba(34, 197, 94, 0.7)' }
        },
        {
          x: puts.map(r => r.strike), y: puts.map(r => r.open_interest),
          name: 'Put Open Interest', type: 'bar', marker: { color: 'rgba(239, 68, 68, 0.7)' }
        }
      ], {
        barmode: 'group',
        shapes: spotShapes,
        title: { text: 'Open Interest Wall by Strike (Calls vs Puts)', font: { size: 13 } },
        xaxis: { title: 'Strike Price ($)' },
        yaxis: { title: 'Open Interest (Contracts)' }
      });

      setStatus(`Loaded ${chain.length} contracts for ${sym} on ${td}`);
      } catch (e) {
        if (thisToken !== smileLoadToken) return;
        setStatus(`Error loading cross-section: ${e.message}`);
      }
    }

    // =========================================================
    // TAB 4: RAW HISTORICAL DATA TABLE & CSV EXPORT
    // =========================================================
    function renderTable(rows) {
      const tbody = document.querySelector('#data-table tbody');
      tbody.innerHTML = '';
      for (const r of [...rows].reverse()) {
        const tr = document.createElement('tr');
        const fmt = (v, d = 2) => (v != null ? Number(v).toFixed(d) : '--');
        tr.innerHTML = `
          <td style="font-weight:600;">${r.trade_date}</td>
          <td>
            <button class="btn-action-view" onclick="jumpFromContractToChain('${r.trade_date}')" title="Inspect ${r.trade_date} Full Option Chain">
              📅 Chain
            </button>
          </td>
          <td>${r.dte ?? '--'}</td>
          <td>$${fmt(r.underlying_price, 2)}</td>
          <td>$${fmt(r.bid, 2)}</td>
          <td>$${fmt(r.ask, 2)}</td>
          <td style="color:var(--accent);font-weight:600">$${fmt(r.mid, 2)}</td>
          <td>$${fmt(r.close, 2)}</td>
          <td>${(r.volume ?? 0).toLocaleString()}</td>
          <td>${(r.open_interest ?? 0).toLocaleString()}</td>
          <td style="color:var(--accent-amber)">${r.implied_vol != null ? (r.implied_vol * 100).toFixed(1) + '%' : '--'}</td>
          <td>${fmt(r.delta, 3)}</td>
          <td>${fmt(r.gamma, 4)}</td>
          <td style="color:var(--accent-red)">${fmt(r.theta, 3)}</td>
          <td style="color:var(--accent-green)">${fmt(r.vega_per_pct ?? r.vega, 3)}</td>
          <td>${fmt(r.rho_per_pct ?? r.rho, 3)}</td>
          <td>${fmt(r.vanna, 4)}</td>
          <td>${fmt(r.charm, 4)}</td>
        `;
        tbody.appendChild(tr);
      }
    }

    async function pullFromGDrive() {
      const sym = document.getElementById('sel-symbol').value;
      const yr = document.getElementById('sel-year').value;
      if (!sym) return;
      setStatus(`☁ Pulling ${sym} ${yr || 'all years'} from Google Drive into local cache...`);
      try {
        const url = `/api/gdrive-pull?symbol=${encodeURIComponent(sym)}` + (yr ? `&year=${yr}` : '');
        const res = await apiGet(url);
        setStatus(`✓ Synced ${res.local_files.length} Parquet files for ${sym} from Google Drive.`);
        await onSymbolChange();
      } catch (e) {
        setStatus(`Google Drive pull failed: ${e.message}`);
      }
    }

    function exportCSV() {
      if (!currentSeries.length) return;
      const headers = Object.keys(currentSeries[0]);
      const csvRows = [headers.join(',')];
      for (const r of currentSeries) {
        csvRows.push(headers.map(h => r[h] ?? '').join(','));
      }
      const blob = new Blob([csvRows.join('\n')], { type: 'text/csv' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      const sym = document.getElementById('sel-symbol').value;
      const exp = document.getElementById('sel-expiration').value;
      const strike = document.getElementById('sel-strike').value;
      const right = document.getElementById('sel-right').value;
      a.download = `${sym}_${exp}_${strike}_${right}.csv`;
      a.click();
    }

    window.addEventListener('DOMContentLoaded', initApp);
  </script>
</body>
</html>
"""


def make_handler(backend: OptionsViewerBackend) -> type[BaseHTTPRequestHandler]:
    """Create an HTTP request handler bound to `backend`."""

    class ViewerRequestHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            logger.debug("%s - %s", self.address_string(), format % args)

        def _send_json(self, payload: Any, status: int = 200) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, html: str, status: int = 200) -> None:
            body = html.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            qs = urllib.parse.parse_qs(parsed.query)

            try:
                if path in ("/", "/index.html"):
                    self._send_html(INDEX_HTML)
                    return

                if path == "/api/symbols":
                    self._send_json({"symbols": backend.list_symbols()})
                    return

                if path in ("/api/metadata", "/api/contracts", "/api/timeseries", "/api/smile", "/api/chain", "/api/gdrive-pull"):
                    sym_raw = qs.get("symbol", ["AAPL"])[0]
                    try:
                        sym = validate_symbol(sym_raw)
                    except ValueError as sym_err:
                        self._send_json({"error": str(sym_err)}, status=400)
                        return

                if path == "/api/metadata":
                    yr_raw = qs.get("year", [None])[0]
                    yr = int(yr_raw) if yr_raw and yr_raw.isdigit() else None
                    self._send_json(backend.get_symbol_metadata(sym, year=yr))
                    return

                if path == "/api/contracts":
                    exp = qs.get("expiration", [""])[0]
                    yr_raw = qs.get("year", [None])[0]
                    yr = int(yr_raw) if yr_raw and yr_raw.isdigit() else None
                    self._send_json(backend.get_contracts_for_expiration(sym, exp, year=yr))
                    return

                if path == "/api/timeseries":
                    exp = qs.get("expiration", [""])[0]
                    strike = float(qs.get("strike", ["100"])[0])
                    right = qs.get("right", ["call"])[0]
                    yr_raw = qs.get("year", [None])[0]
                    yr = int(yr_raw) if yr_raw and yr_raw.isdigit() else None
                    self._send_json(backend.get_contract_timeseries(sym, exp, strike, right, year=yr))
                    return

                if path == "/api/smile":
                    td = qs.get("trade_date", [""])[0]
                    exp = qs.get("expiration", [None])[0]
                    self._send_json(backend.get_chain_smile(sym, td, expiration=exp))
                    return

                if path == "/api/chain":
                    td = qs.get("trade_date", [None])[0]
                    exp = qs.get("expiration", [None])[0]
                    self._send_json(backend.get_option_chain(sym, trade_date=td, expiration=exp))
                    return

                if path == "/api/gdrive-pull":
                    yr_raw = qs.get("year", [None])[0]
                    yr = int(yr_raw) if yr_raw and yr_raw.isdigit() else None
                    self._send_json(backend.fetch_from_gdrive(sym, year=yr))
                    return

                self._send_json({"error": f"Not found: {path}"}, status=404)
            except Exception as exc:
                logger.exception("Error handling %s: %s", self.path, exc)
                self._send_json({"error": str(exc)}, status=500)

    return ViewerRequestHandler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="options-viewer",
        description="Launch the local Options Price & Greeks Interactive Web Dashboard.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host interface to bind (default: 127.0.0.1; use 0.0.0.0 for LAN/Tailscale access).",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=8050,
        help="Local port to serve the dashboard on (default: 8050).",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Path to Parquet data directory or mounted Google Drive folder (default: ./data).",
    )
    parser.add_argument(
        "--universe-file",
        type=Path,
        default=DEFAULT_UNIVERSE_CSV,
        help=f"Path to S&P 500 Universe CSV (default: {DEFAULT_UNIVERSE_CSV}).",
    )
    parser.add_argument(
        "--no-gdrive-fetch",
        action="store_true",
        help="Disable automatic on-demand downloading from Google Drive when a Parquet file is missing locally.",
    )
    parser.add_argument(
        "--gdrive-folder-id",
        default=os.getenv("GDRIVE_FOLDER_ID", DEFAULT_GDRIVE_FOLDER_ID),
        help="Google Drive root folder ID for on-demand Parquet caching.",
    )
    parser.add_argument(
        "--open-browser",
        action="store_true",
        help="Automatically open the web dashboard in the default browser.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)

    backend = OptionsViewerBackend(
        data_dir=args.data_dir,
        universe_csv=args.universe_file,
        auto_fetch_gdrive=not args.no_gdrive_fetch,
        gdrive_folder_id=args.gdrive_folder_id,
    )

    server = ThreadingHTTPServer((args.host, args.port), make_handler(backend))
    url = f"http://{args.host}:{args.port}"
    console.rule("[bold cyan]ThetaData Options & Greeks Interactive Dashboard[/bold cyan]")
    console.print(
        f"  URL             : [bold green]{url}[/bold green]\n"
        f"  Data Directory  : {args.data_dir.resolve()}\n"
        f"  Hybrid GDrive   : {'Enabled (On-Demand Cache)' if not args.no_gdrive_fetch else 'Disabled'}\n"
        "  Press [bold]Ctrl+C[/bold] to stop the server."
    )

    if args.open_browser:
        webbrowser.open(url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        console.print("\n[yellow]Shutting down Options Viewer...[/yellow]")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
