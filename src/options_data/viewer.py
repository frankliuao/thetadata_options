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
        sym = symbol.strip().upper()
        years_filter = [year] if year is not None else None
        lf = self.loader.scan_symbol(sym, years=years_filter)

        # Collect available years from all local files first
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
        sym = symbol.strip().upper()
        exp_date = datetime.date.fromisoformat(expiration)
        years_filter = [year] if year is not None else None
        lf = self.loader.scan_symbol(sym, years=years_filter).filter(pl.col("expiration") == exp_date)

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

        # Find ATM strike closest to underlying_ref
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
        sym = symbol.strip().upper()
        exp_date = datetime.date.fromisoformat(expiration)
        right_clean = right.strip().lower()
        # Note: a contract expiring in `exp_date` may have traded in `exp_date.year` and `exp_date.year - 1`
        years_filter = [year] if year is not None else None

        lf = (
            self.loader.scan_symbol(sym, years=years_filter)
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
        sym = symbol.strip().upper()
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

    def fetch_from_gdrive(self, symbol: str, year: int | None = None) -> dict[str, Any]:
        """Explicitly pull `symbol` (and optional `year`) from Google Drive into local cache."""
        sym = symbol.strip().upper()
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
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
      --accent: #38bdf8;
      --accent-green: #22c55e;
      --accent-red: #ef4444;
      --accent-purple: #a855f7;
      --accent-amber: #f59e0b;
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
      padding: 12px 20px;
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
      font-size: 16px;
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
      padding: 12px 20px;
      display: flex;
      flex-wrap: wrap;
      align-items: flex-end;
      gap: 14px;
    }
    .field {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .field label {
      font-size: 11px;
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
      padding: 7px 10px;
      font-size: 13px;
      min-width: 120px;
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
      padding: 7px 14px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
      transition: opacity 0.15s;
    }
    button:hover { opacity: 0.9; }
    button.secondary {
      background: var(--bg-card);
      color: var(--text-main);
      border: 1px solid var(--border);
    }
    button.secondary.active {
      border-color: var(--accent);
      color: var(--accent);
      background: rgba(56, 189, 248, 0.12);
    }
    .tabs {
      display: flex;
      gap: 8px;
      padding: 12px 20px 0;
      border-bottom: 1px solid var(--border);
      background: var(--bg-primary);
    }
    .tab-btn {
      background: transparent;
      color: var(--text-muted);
      border: none;
      border-bottom: 2px solid transparent;
      border-radius: 0;
      padding: 8px 16px;
      font-size: 13px;
      font-weight: 600;
    }
    .tab-btn.active {
      color: var(--accent);
      border-bottom-color: var(--accent);
    }
    .kpi-strip {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(145px, 1fr));
      gap: 10px;
      padding: 14px 20px;
    }
    .kpi-card {
      background: var(--bg-panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 10px 12px;
    }
    .kpi-title {
      font-size: 11px;
      color: var(--text-muted);
      text-transform: uppercase;
    }
    .kpi-value {
      font-size: 18px;
      font-weight: 700;
      margin-top: 3px;
    }
    .kpi-sub {
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 2px;
    }
    .grid-charts {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
      padding: 0 20px 20px;
    }
    @media (max-width: 1100px) {
      .grid-charts { grid-template-columns: 1fr; }
    }
    .chart-box {
      background: var(--bg-panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 10px;
      min-height: 360px;
    }
    .chart-box.full-width {
      grid-column: 1 / -1;
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
    th, td {
      padding: 7px 10px;
      border-bottom: 1px solid #1e293b;
      text-align: right;
      white-space: nowrap;
    }
    th {
      background: var(--bg-card);
      color: var(--text-muted);
      font-weight: 600;
      position: sticky;
      top: 0;
    }
    th:first-child, td:first-child { text-align: left; }
    tr:hover td { background: rgba(56, 189, 248, 0.05); }
    .status-bar {
      font-size: 12px;
      color: var(--text-muted);
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .hidden { display: none !important; }
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

  <div class="toolbar">
    <div class="field">
      <label>Symbol (S&P 500)</label>
      <select id="sel-symbol" onchange="onSymbolChange()" style="min-width:190px;"></select>
    </div>
    <div class="field">
      <label>Year Filter</label>
      <select id="sel-year" onchange="onYearChange()"></select>
    </div>
    <div class="field" id="fld-expiration">
      <label>Expiration Date</label>
      <select id="sel-expiration" onchange="onExpirationChange()"></select>
    </div>
    <div class="field" id="fld-right">
      <label>Option Type</label>
      <select id="sel-right" onchange="loadContractSeries()">
        <option value="call">CALL</option>
        <option value="put">PUT</option>
      </select>
    </div>
    <div class="field" id="fld-strike">
      <label>Strike Price</label>
      <select id="sel-strike" onchange="loadContractSeries()"></select>
    </div>
    <div class="field" id="fld-quick">
      <label>Quick Pick</label>
      <div style="display:flex; gap:5px;">
        <button class="secondary" onclick="pickATM()">ATM</button>
        <button class="secondary" onclick="pickMaxOI()">Max OI</button>
      </div>
    </div>
    <div class="field hidden" id="fld-tradedate">
      <label>Trade Date (Snapshot)</label>
      <select id="sel-tradedate" onchange="loadChainSmile()"></select>
    </div>
  </div>

  <div class="tabs">
    <button class="tab-btn active" id="tab-btn-lifecycle" onclick="switchTab('lifecycle')">📈 Contract Price & Greeks Lifecycle</button>
    <button class="tab-btn" id="tab-btn-smile" onclick="switchTab('smile')">🌐 IV Smile & Strike Greeks Cross-Section</button>
    <button class="tab-btn" id="tab-btn-table" onclick="switchTab('table')">📋 Raw Contract Data & CSV Export</button>
  </div>

  <!-- KPI Summary Strip -->
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

  <!-- TAB 1: Contract Lifecycle Charts -->
  <div id="view-lifecycle" class="grid-charts">
    <div class="chart-box full-width" id="chart-price" style="min-height:400px;"></div>
    <div class="chart-box" id="chart-greeks-1"></div>
    <div class="chart-box" id="chart-greeks-2"></div>
    <div class="chart-box" id="chart-iv-oi"></div>
    <div class="chart-box" id="chart-higher-greeks"></div>
  </div>

  <!-- TAB 2: Option Chain IV Smile & Cross-Section -->
  <div id="view-smile" class="grid-charts hidden">
    <div class="chart-box" id="chart-smile-iv"></div>
    <div class="chart-box" id="chart-smile-delta"></div>
    <div class="chart-box" id="chart-smile-gex"></div>
    <div class="chart-box" id="chart-smile-oi"></div>
  </div>

  <!-- TAB 3: Data Table -->
  <div id="view-table" class="table-container hidden">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
      <span style="color:var(--text-muted)">Showing daily EOD + Black-Scholes Greeks records</span>
      <button onclick="exportCSV()">⬇ Download CSV</button>
    </div>
    <table id="data-table">
      <thead>
        <tr>
          <th>Date</th><th>DTE</th><th>Spot ($)</th><th>Bid</th><th>Ask</th><th>Mid</th><th>Close</th>
          <th>Vol</th><th>Open Int</th><th>IV (%)</th><th>Delta</th><th>Gamma</th><th>Theta</th>
          <th>Vega/1%</th><th>Rho/1%</th><th>Vanna</th><th>Charm</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>

  <script>
    let currentTab = 'lifecycle';
    let currentMeta = null;
    let currentContractsMeta = null;
    let currentSeries = [];

    const plotLayoutBase = {
      paper_bgcolor: '#111827',
      plot_bgcolor: '#111827',
      font: { color: '#e2e8f0', size: 11 },
      margin: { l: 55, r: 50, t: 42, b: 40 },
      xaxis: { gridcolor: '#1e293b', zerolinecolor: '#334155' },
      yaxis: { gridcolor: '#1e293b', zerolinecolor: '#334155' },
      legend: { orientation: 'h', y: 1.12, x: 0 },
      hovermode: 'x unified'
    };

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

    async function initApp() {
      setStatus('Loading universe symbols...');
      const data = await apiGet('/api/symbols');
      const sel = document.getElementById('sel-symbol');
      sel.innerHTML = '';
      let defaultSym = 'AAPL';
      const syms = data.symbols || [];
      if (!syms.some(s => s.symbol === defaultSym) && syms.length > 0) {
        defaultSym = syms[0].symbol;
      }
      for (const item of syms) {
        const opt = document.createElement('option');
        opt.value = item.symbol;
        const cacheTag = item.cached_count > 0 ? `[${item.cached_count}y local]` : `[☁ Cloud]`;
        opt.textContent = `${item.symbol} - ${item.company_name.slice(0, 22)} ${cacheTag}`;
        if (item.symbol === defaultSym) opt.selected = true;
        sel.appendChild(opt);
      }
      await onSymbolChange();
    }

    async function onSymbolChange() {
      const sym = document.getElementById('sel-symbol').value;
      if (!sym) return;
      setStatus(`Scanning Parquet metadata for ${sym}...`);
      try {
        const meta = await apiGet(`/api/metadata?symbol=${encodeURIComponent(sym)}`);
        currentMeta = meta;
        const yearSel = document.getElementById('sel-year');
        yearSel.innerHTML = '';
        const years = meta.available_years || [];
        const latestYr = years.length > 0 ? years[years.length - 1] : 2026;
        for (const yr of [...years].reverse()) {
          const opt = document.createElement('option');
          opt.value = yr;
          opt.textContent = yr;
          if (yr === latestYr) opt.selected = true;
          yearSel.appendChild(opt);
        }
        const allOpt = document.createElement('option');
        allOpt.value = '';
        allOpt.textContent = 'All Years (2016-2026)';
        yearSel.appendChild(allOpt);

        document.getElementById('cache-badge').textContent = `${sym}: ${years.length} Yearly Parquets Cached`;
        await onYearChange();
      } catch (e) {
        setStatus(`Error loading ${sym}: ${e.message}`);
      }
    }

    async function onYearChange() {
      const sym = document.getElementById('sel-symbol').value;
      const yr = document.getElementById('sel-year').value;
      setStatus(`Loading expirations & dates for ${sym} ${yr || 'All'}...`);
      const url = `/api/metadata?symbol=${encodeURIComponent(sym)}` + (yr ? `&year=${yr}` : '');
      const meta = await apiGet(url);
      currentMeta = meta;

      const expSel = document.getElementById('sel-expiration');
      expSel.innerHTML = '';
      const exps = meta.expirations || [];
      // Default to an expiration roughly in the middle or recent
      const defaultIdx = Math.max(0, Math.min(exps.length - 1, Math.floor(exps.length * 0.7)));
      exps.forEach((ex, i) => {
        const opt = document.createElement('option');
        opt.value = ex;
        opt.textContent = ex;
        if (i === defaultIdx) opt.selected = true;
        expSel.appendChild(opt);
      });

      const tdSel = document.getElementById('sel-tradedate');
      tdSel.innerHTML = '';
      const tds = meta.trade_dates || [];
      for (let i = tds.length - 1; i >= 0; i--) {
        const opt = document.createElement('option');
        opt.value = tds[i];
        opt.textContent = tds[i];
        tdSel.appendChild(opt);
      }

      if (currentTab === 'smile') {
        await loadChainSmile();
      } else {
        await onExpirationChange();
      }
    }

    async function onExpirationChange() {
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
      await loadContractSeries();
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

    async function loadContractSeries() {
      const sym = document.getElementById('sel-symbol').value;
      const yr = document.getElementById('sel-year').value;
      const exp = document.getElementById('sel-expiration').value;
      const strike = document.getElementById('sel-strike').value;
      const right = document.getElementById('sel-right').value;
      if (!sym || !exp || !strike) return;

      setStatus(`Querying ${sym} ${exp} $${strike} ${right.toUpperCase()}...`);
      const url = `/api/timeseries?symbol=${encodeURIComponent(sym)}&expiration=${encodeURIComponent(exp)}&strike=${encodeURIComponent(strike)}&right=${encodeURIComponent(right)}` + (yr ? `&year=${yr}` : '');
      const data = await apiGet(url);
      currentSeries = data.series || [];
      renderLifecycleCharts(data);
      renderTable(currentSeries);
      setStatus(`Loaded ${currentSeries.length} trading days for ${sym} ${exp} $${strike} ${right.toUpperCase()}`);
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
      Plotly.newPlot('chart-price', [
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
        ...plotLayoutBase,
        title: { text: `${data.symbol} ${data.expiration} $${data.strike} ${data.right.toUpperCase()} — Option Price vs Underlying Spot`, font: { size: 13 } },
        yaxis: { ...plotLayoutBase.yaxis, title: 'Option Price ($)' },
        yaxis2: {
          title: 'Underlying Price ($)',
          overlaying: 'y',
          side: 'right',
          gridcolor: 'transparent'
        }
      }, { responsive: true });

      // Chart 2: Delta & Gamma
      Plotly.newPlot('chart-greeks-1', [
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
        ...plotLayoutBase,
        title: { text: 'Directional & Convexity Greeks: Delta (Δ) & Gamma (Γ)', font: { size: 13 } },
        yaxis: { ...plotLayoutBase.yaxis, title: 'Delta' },
        yaxis2: { title: 'Gamma', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      }, { responsive: true });

      // Chart 3: Theta, Vega_per_pct, Rho_per_pct
      Plotly.newPlot('chart-greeks-2', [
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
        ...plotLayoutBase,
        title: { text: 'Time Decay & Volatility Sensitivity: Theta (Θ), Vega & Rho', font: { size: 13 } },
        yaxis: { ...plotLayoutBase.yaxis, title: 'Theta ($/day)' },
        yaxis2: { title: 'Vega / Rho ($ per 1%)', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      }, { responsive: true });

      // Chart 4: Implied Volatility + Open Interest & Volume
      Plotly.newPlot('chart-iv-oi', [
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
        ...plotLayoutBase,
        barmode: 'overlay',
        title: { text: 'Implied Volatility (%) & Open Interest / Volume', font: { size: 13 } },
        yaxis: { ...plotLayoutBase.yaxis, title: 'Implied Vol (%)' },
        yaxis2: { title: 'Contracts (OI / Volume)', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      }, { responsive: true });

      // Chart 5: Higher-Order Greeks (Vanna, Charm, Vomma)
      Plotly.newPlot('chart-higher-greeks', [
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
        ...plotLayoutBase,
        title: { text: 'Second-Order Greeks: Vanna, Charm & Vomma', font: { size: 13 } },
        yaxis: { ...plotLayoutBase.yaxis, title: 'Vanna / Charm' },
        yaxis2: { title: 'Vomma', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      }, { responsive: true });
    }

    async function loadChainSmile() {
      const sym = document.getElementById('sel-symbol').value;
      const td = document.getElementById('sel-tradedate').value;
      const exp = document.getElementById('sel-expiration').value;
      if (!sym || !td) return;

      setStatus(`Loading option chain cross-section for ${sym} on ${td}...`);
      const url = `/api/smile?symbol=${encodeURIComponent(sym)}&trade_date=${encodeURIComponent(td)}&expiration=${encodeURIComponent(exp)}`;
      const data = await apiGet(url);

      // Update expiration dropdown if needed
      const availExps = data.available_expirations || [];
      const expSel = document.getElementById('sel-expiration');
      if (availExps.length && !availExps.includes(exp)) {
        expSel.innerHTML = '';
        availExps.forEach(e => {
          const opt = document.createElement('option');
          opt.value = e;
          opt.textContent = e;
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

      // Smile 1: IV vs Strike
      Plotly.newPlot('chart-smile-iv', [
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
        ...plotLayoutBase,
        shapes: spotShapes,
        title: { text: `Volatility Smile (${sym} on ${td}, Exp ${data.selected_expiration}, Spot $${spot ?? '--'})`, font: { size: 13 } },
        xaxis: { ...plotLayoutBase.xaxis, title: 'Strike Price ($)' },
        yaxis: { ...plotLayoutBase.yaxis, title: 'Implied Volatility (%)' }
      }, { responsive: true });

      // Smile 2: Delta & Gamma across Strikes
      Plotly.newPlot('chart-smile-delta', [
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
        ...plotLayoutBase,
        shapes: spotShapes,
        title: { text: 'Delta & Gamma Profile by Strike', font: { size: 13 } },
        xaxis: { ...plotLayoutBase.xaxis, title: 'Strike Price ($)' },
        yaxis: { ...plotLayoutBase.yaxis, title: 'Delta' },
        yaxis2: { title: 'Gamma', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      }, { responsive: true });

      // Smile 3: Theta & Vega across Strikes
      Plotly.newPlot('chart-smile-gex', [
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
        ...plotLayoutBase,
        shapes: spotShapes,
        title: { text: 'Vega & Theta Decay Across Strikes', font: { size: 13 } },
        xaxis: { ...plotLayoutBase.xaxis, title: 'Strike Price ($)' },
        yaxis: { ...plotLayoutBase.yaxis, title: 'Vega ($/1%)' },
        yaxis2: { title: 'Theta ($/day)', overlaying: 'y', side: 'right', gridcolor: 'transparent' }
      }, { responsive: true });

      // Smile 4: Call vs Put Open Interest Wall
      Plotly.newPlot('chart-smile-oi', [
        {
          x: calls.map(r => r.strike), y: calls.map(r => r.open_interest),
          name: 'Call Open Interest', type: 'bar', marker: { color: 'rgba(34, 197, 94, 0.7)' }
        },
        {
          x: puts.map(r => r.strike), y: puts.map(r => r.open_interest),
          name: 'Put Open Interest', type: 'bar', marker: { color: 'rgba(239, 68, 68, 0.7)' }
        }
      ], {
        ...plotLayoutBase,
        barmode: 'group',
        shapes: spotShapes,
        title: { text: 'Open Interest Wall by Strike (Calls vs Puts)', font: { size: 13 } },
        xaxis: { ...plotLayoutBase.xaxis, title: 'Strike Price ($)' },
        yaxis: { ...plotLayoutBase.yaxis, title: 'Open Interest (Contracts)' }
      }, { responsive: true });

      setStatus(`Loaded ${chain.length} contracts for ${sym} on ${td}`);
    }

    function renderTable(rows) {
      const tbody = document.querySelector('#data-table tbody');
      tbody.innerHTML = '';
      for (const r of [...rows].reverse()) {
        const tr = document.createElement('tr');
        const fmt = (v, d = 2) => (v != null ? Number(v).toFixed(d) : '--');
        tr.innerHTML = `
          <td>${r.trade_date}</td>
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

    function switchTab(tab) {
      currentTab = tab;
      document.getElementById('tab-btn-lifecycle').classList.toggle('active', tab === 'lifecycle');
      document.getElementById('tab-btn-smile').classList.toggle('active', tab === 'smile');
      document.getElementById('tab-btn-table').classList.toggle('active', tab === 'table');

      document.getElementById('view-lifecycle').classList.toggle('hidden', tab !== 'lifecycle');
      document.getElementById('view-smile').classList.toggle('hidden', tab !== 'smile');
      document.getElementById('view-table').classList.toggle('hidden', tab !== 'table');

      document.getElementById('fld-tradedate').classList.toggle('hidden', tab !== 'smile');
      document.getElementById('fld-right').classList.toggle('hidden', tab === 'smile');
      document.getElementById('fld-strike').classList.toggle('hidden', tab === 'smile');
      document.getElementById('fld-quick').classList.toggle('hidden', tab === 'smile');

      if (tab === 'smile') {
        loadChainSmile();
      } else if (tab === 'lifecycle') {
        loadContractSeries();
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

                if path == "/api/metadata":
                    sym = qs.get("symbol", ["AAPL"])[0]
                    yr_raw = qs.get("year", [None])[0]
                    yr = int(yr_raw) if yr_raw and yr_raw.isdigit() else None
                    self._send_json(backend.get_symbol_metadata(sym, year=yr))
                    return

                if path == "/api/contracts":
                    sym = qs.get("symbol", ["AAPL"])[0]
                    exp = qs.get("expiration", [""])[0]
                    yr_raw = qs.get("year", [None])[0]
                    yr = int(yr_raw) if yr_raw and yr_raw.isdigit() else None
                    self._send_json(backend.get_contracts_for_expiration(sym, exp, year=yr))
                    return

                if path == "/api/timeseries":
                    sym = qs.get("symbol", ["AAPL"])[0]
                    exp = qs.get("expiration", [""])[0]
                    strike = float(qs.get("strike", ["100"])[0])
                    right = qs.get("right", ["call"])[0]
                    yr_raw = qs.get("year", [None])[0]
                    yr = int(yr_raw) if yr_raw and yr_raw.isdigit() else None
                    self._send_json(backend.get_contract_timeseries(sym, exp, strike, right, year=yr))
                    return

                if path == "/api/smile":
                    sym = qs.get("symbol", ["AAPL"])[0]
                    td = qs.get("trade_date", [""])[0]
                    exp = qs.get("expiration", [None])[0]
                    self._send_json(backend.get_chain_smile(sym, td, expiration=exp))
                    return

                if path == "/api/gdrive-pull":
                    sym = qs.get("symbol", ["AAPL"])[0]
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
        default="0.0.0.0",
        help="Host interface to bind (default: 0.0.0.0 for local + Tailscale access).",
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
