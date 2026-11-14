#!/usr/bin/env python3
"""Generate high-fidelity synthetic mock options data and export static GitHub Pages demo."""

from __future__ import annotations

import datetime
import json
import math
import re
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
DOCS_DIR = ROOT_DIR / "docs"
SAMPLE_DATA_DIR = DOCS_DIR / "sample_data"
SRC_VIEWER_PY = ROOT_DIR / "src" / "options_data" / "viewer.py"


def norm_cdf(x: float) -> float:
    """Cumulative distribution function for standard normal distribution."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def norm_pdf(x: float) -> float:
    """Probability density function for standard normal distribution."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def compute_bs_greeks(
    spot: float,
    strike: float,
    dte: int,
    r: float,
    iv: float,
    right: str,
) -> dict[str, float]:
    """Compute Black-Scholes option price and 1st/2nd order Greeks."""
    tte = max(dte / 365.0, 1e-5)
    sqrt_t = math.sqrt(tte)

    d1 = (math.log(spot / strike) + (r + 0.5 * iv * iv) * tte) / (iv * sqrt_t)
    d2 = d1 - iv * sqrt_t

    nd1 = norm_cdf(d1)
    nd2 = norm_cdf(d2)
    pdf_d1 = norm_pdf(d1)
    disc = math.exp(-r * tte)

    gamma = pdf_d1 / (spot * iv * sqrt_t)
    vega = spot * sqrt_t * pdf_d1
    vega_per_pct = vega / 100.0

    vanna = -pdf_d1 * d2 / iv
    vomma = vega * d1 * d2 / iv

    if right == "call":
        price = spot * nd1 - strike * disc * nd2
        delta = nd1
        theta_annual = -(spot * pdf_d1 * iv) / (2.0 * sqrt_t) - r * strike * disc * nd2
        rho = strike * tte * disc * nd2
        charm = -pdf_d1 * (r / (iv * sqrt_t) - d2 / (2.0 * tte))
    else:
        n_neg_d1 = norm_cdf(-d1)
        n_neg_d2 = norm_cdf(-d2)
        price = strike * disc * n_neg_d2 - spot * n_neg_d1
        delta = nd1 - 1.0
        theta_annual = -(spot * pdf_d1 * iv) / (2.0 * sqrt_t) + r * strike * disc * n_neg_d2
        rho = -strike * tte * disc * n_neg_d2
        charm = -pdf_d1 * (r / (iv * sqrt_t) - d2 / (2.0 * tte))

    theta = theta_annual / 365.0
    rho_per_pct = rho / 100.0

    return {
        "price": max(0.01, round(price, 4)),
        "delta": round(delta, 5),
        "gamma": round(gamma, 6),
        "theta": round(theta, 5),
        "vega": round(vega, 5),
        "vega_per_pct": round(vega_per_pct, 5),
        "rho": round(rho, 5),
        "rho_per_pct": round(rho_per_pct, 5),
        "vanna": round(vanna, 6),
        "charm": round(charm, 6),
        "vomma": round(vomma, 6),
    }


def generate_business_days(start_date: datetime.date, end_date: datetime.date) -> list[datetime.date]:
    """Generate all Monday-Friday business dates between start_date and end_date."""
    dates: list[datetime.date] = []
    curr = start_date
    while curr <= end_date:
        if curr.weekday() < 5:  # Mon-Fri
            dates.append(curr)
        curr += datetime.timedelta(days=1)
    return dates


def generate_synthetic_symbol_data(
    symbol: str,
    base_spot: float,
    strikes: list[float],
    base_iv: float,
    start_date: datetime.date,
    expiration_date: datetime.date,
    r: float = 0.045,
) -> list[dict[str, Any]]:
    """Generate realistic daily simulated option chain and Greeks for a single symbol."""
    trade_dates = generate_business_days(start_date, expiration_date)
    records: list[dict[str, Any]] = []

    seed_val = sum(ord(c) for c in symbol)

    spot_history: dict[datetime.date, float] = {}
    for i, td in enumerate(trade_dates):
        sin_osc = math.sin((i + seed_val % 7) * 0.45) * 1.8
        cos_osc = math.cos((i * 1.3 + seed_val % 5)) * 1.2
        drift = (i - len(trade_dates) / 2) * 0.15
        spot = round(base_spot + sin_osc + cos_osc + drift, 2)
        spot_history[td] = spot

    for td in trade_dates:
        spot = spot_history[td]
        dte = (expiration_date - td).days

        for strike in strikes:
            moneyness = (strike - spot) / spot
            skew = -0.28 * moneyness + 0.45 * (moneyness**2)
            iv = max(0.10, min(1.20, base_iv * (1.0 + skew)))

            atm_distance = abs(strike - spot) / spot
            oi_factor = max(0.15, 1.0 - atm_distance * 3.5)
            base_oi = int(5000 * oi_factor * (1.0 + 0.4 * math.sin(strike + seed_val)))
            oi_growth = 1.0 + (len(trade_dates) - dte) * 0.04
            open_interest = int(base_oi * oi_growth)
            prior_oi = int(open_interest * 0.96)

            for right in ("call", "put"):
                greeks = compute_bs_greeks(spot, strike, dte, r, iv, right)
                theo_price = greeks["price"]

                spread = max(0.04, round(theo_price * 0.035 + 0.04, 2))
                bid = max(0.01, round(theo_price - spread / 2.0, 2))
                ask = round(bid + spread, 2)
                mid = round((bid + ask) / 2.0, 2)
                close = mid
                open_p = round(mid * (1.0 + 0.005 * math.sin(dte * 2.1 + strike)), 2)
                high_p = max(open_p, close, round(mid * 1.025, 2))
                low_p = min(open_p, close, round(mid * 0.975, 2))

                vol_factor = max(0.08, 1.0 - atm_distance * 4.0)
                volume = int(max(10, 1800 * vol_factor * (1.0 + 0.5 * math.cos(dte + seed_val))))
                count = max(5, volume // 4)

                records.append(
                    {
                        "trade_date": td.isoformat(),
                        "symbol": symbol,
                        "expiration": expiration_date.isoformat(),
                        "dte": dte,
                        "strike": strike,
                        "right": right,
                        "underlying_price": spot,
                        "open": open_p,
                        "high": high_p,
                        "low": low_p,
                        "close": close,
                        "bid": bid,
                        "ask": ask,
                        "mid": mid,
                        "spread": spread,
                        "volume": volume,
                        "count": count,
                        "open_interest": open_interest,
                        "open_interest_prior_close": prior_oi,
                        "implied_vol": round(iv, 4),
                        "delta": greeks["delta"],
                        "gamma": greeks["gamma"],
                        "theta": greeks["theta"],
                        "vega": greeks["vega"],
                        "vega_per_pct": greeks["vega_per_pct"],
                        "rho": greeks["rho"],
                        "rho_per_pct": greeks["rho_per_pct"],
                        "vanna": greeks["vanna"],
                        "charm": greeks["charm"],
                        "vomma": greeks["vomma"],
                    }
                )

    return records


def build_symbols_metadata() -> list[dict[str, Any]]:
    """Metadata for the 3 demo symbols."""
    return [
        {
            "symbol": "AAPL",
            "company_name": "Apple Inc. [Synthetic Mock]",
            "cached_years": [2026],
            "cached_count": 1,
            "is_mock": True,
        },
        {
            "symbol": "MSFT",
            "company_name": "Microsoft Corp. [Synthetic Mock]",
            "cached_years": [2026],
            "cached_count": 1,
            "is_mock": True,
        },
        {
            "symbol": "NVDA",
            "company_name": "NVIDIA Corp. [Synthetic Mock]",
            "cached_years": [2026],
            "cached_count": 1,
            "is_mock": True,
        },
    ]


DISCLAIMER_BANNER_HTML = """
  <!-- TOP GLOBAL NOTICE: SYNTHETIC MOCK DATA DISCLAIMER -->
  <div class="mock-disclaimer-banner">
    <div class="mock-disclaimer-content">
      <span class="mock-disclaimer-icon">⚠️</span>
      <div>
        <span class="mock-disclaimer-badge">DEMO MODE</span>
        <strong>NOTICE: 100% SYNTHETIC MOCK DATA ONLY</strong> — All options chains, prices, implied volatilities, and Greeks displayed on this page are synthetically simulated mock data generated via Black-Scholes pricing models for UI demonstration. No real, proprietary, or commercial market data from ThetaData or OPRA is stored or distributed here.
      </div>
    </div>
    <div style="display:flex; align-items:center; gap:8px;">
      <a href="https://github.com/frankliuao/thetadata_options" target="_blank" class="mock-disclaimer-btn">⭐ View GitHub Repo ↗</a>
    </div>
  </div>
"""

DISCLAIMER_CSS = """
    .mock-disclaimer-banner {
      background: linear-gradient(90deg, #451a03 0%, #1c1917 100%);
      border-bottom: 2px solid #f59e0b;
      color: #fef3c7;
      padding: 10px 20px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      font-size: 12px;
      line-height: 1.45;
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
      position: sticky;
      top: 0;
      z-index: 99999;
    }
    .mock-disclaimer-content {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .mock-disclaimer-icon {
      font-size: 20px;
      line-height: 1;
      flex-shrink: 0;
    }
    .mock-disclaimer-badge {
      background: #f59e0b;
      color: #000;
      font-weight: 800;
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      padding: 2px 7px;
      border-radius: 4px;
      margin-right: 6px;
      display: inline-block;
    }
    .mock-disclaimer-btn {
      background: rgba(245, 158, 11, 0.2);
      color: #fbbf24;
      border: 1px solid rgba(245, 158, 11, 0.4);
      padding: 5px 12px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
      text-decoration: none;
      white-space: nowrap;
      transition: all 0.15s;
    }
    .mock-disclaimer-btn:hover {
      background: #f59e0b;
      color: #000;
    }
    .badge-amber {
      background: rgba(245, 158, 11, 0.18);
      color: #f59e0b;
      border: 1px solid rgba(245, 158, 11, 0.45);
    }
    footer.app-footer {
      background: var(--bg-panel);
      border-top: 1px solid var(--border);
      padding: 16px 20px;
      margin-top: 24px;
      color: var(--text-muted);
      font-size: 12px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 12px;
    }
"""

FOOTER_HTML = """
  <footer class="app-footer">
    <div>
      <span style="color:var(--accent-amber); font-weight:700;">⚠️ Synthetic Data Notice:</span>
      All option chains, quotes, IV, and Greeks shown on this static page are 100% synthetically simulated mock data for demonstration. No proprietary market data is redistributed.
    </div>
    <div>
      <a href="https://github.com/frankliuao/thetadata_options" target="_blank" style="color:var(--accent); text-decoration:none; font-weight:600;">
        ThetaData Options Pipeline & Lakehouse on GitHub ↗
      </a>
    </div>
  </footer>
"""

IN_BROWSER_ROUTER_JS = """
    // In-memory cache for client-side static demonstration on GitHub Pages
    const inMemoryCache = {};

    async function apiGet(path) {
      // Direct mock client router for GitHub Pages
      return await mockApiGet(path);
    }

    async function mockApiGet(path) {
      const url = new URL(path, window.location.href);
      const pathname = url.pathname;
      const endpoint = pathname.endsWith('/api/symbols') ? '/api/symbols' :
                       pathname.endsWith('/api/metadata') ? '/api/metadata' :
                       pathname.endsWith('/api/contracts') ? '/api/contracts' :
                       pathname.endsWith('/api/timeseries') ? '/api/timeseries' :
                       pathname.endsWith('/api/smile') ? '/api/smile' :
                       pathname.endsWith('/api/chain') ? '/api/chain' :
                       pathname.endsWith('/api/gdrive-pull') ? '/api/gdrive-pull' : path;
      
      const params = Object.fromEntries(url.searchParams.entries());

      if (endpoint === '/api/symbols' || path.startsWith('/api/symbols')) {
        const res = await fetch('sample_data/symbols.json');
        if (!res.ok) throw new Error('Failed to load sample symbols.json');
        return await res.json();
      }

      const sym = (params.symbol || 'AAPL').toUpperCase();
      if (!inMemoryCache[sym]) {
        setStatus(`Loading synthetic dataset for ${sym}...`);
        const res = await fetch(`sample_data/${sym}.json`);
        if (!res.ok) {
          throw new Error(`Sample data for ${sym} is not included in this static demo. Available demo symbols: AAPL, MSFT, NVDA.`);
        }
        inMemoryCache[sym] = await res.json();
      }
      const records = inMemoryCache[sym];

      if (endpoint === '/api/metadata' || path.startsWith('/api/metadata')) {
        const expSet = new Set();
        const tdSet = new Set();
        const yrSet = new Set();
        for (const r of records) {
          if (r.expiration) expSet.add(r.expiration);
          if (r.trade_date) {
            tdSet.add(r.trade_date);
            yrSet.add(parseInt(r.trade_date.slice(0, 4)));
          }
        }
        const expirations = [...expSet].sort();
        const trade_dates = [...tdSet].sort();
        const available_years = [...yrSet].sort();
        return {
          symbol: sym,
          available_years,
          selected_year: params.year ? parseInt(params.year) : null,
          expirations,
          trade_dates,
          local_files_count: 1,
        };
      }

      if (endpoint === '/api/contracts' || path.startsWith('/api/contracts')) {
        const exp = params.expiration;
        const subset = exp ? records.filter(r => r.expiration === exp) : records;
        const strikeMap = {};
        for (const r of subset) {
          const key = `${r.strike}_${r.right}`;
          if (!strikeMap[key]) {
            strikeMap[key] = {
              strike: r.strike,
              right: r.right,
              days_count: 0,
              first_date: r.trade_date,
              last_date: r.trade_date,
              max_oi: r.open_interest || 0,
              total_volume: 0,
              last_underlying: r.underlying_price,
              last_delta: r.delta
            };
          }
          const item = strikeMap[key];
          item.days_count += 1;
          if (r.trade_date < item.first_date) item.first_date = r.trade_date;
          if (r.trade_date >= item.last_date) {
            item.last_date = r.trade_date;
            item.last_underlying = r.underlying_price;
            item.last_delta = r.delta;
          }
          if ((r.open_interest || 0) > item.max_oi) item.max_oi = r.open_interest;
          item.total_volume += (r.volume || 0);
        }
        const contracts = Object.values(strikeMap).sort((a,b) => a.strike - b.strike || a.right.localeCompare(b.right));
        const strikes = [...new Set(contracts.map(c => c.strike))].sort((a,b) => a - b);
        let underlying_ref = null;
        for (const c of contracts) {
          if (c.last_underlying && c.last_underlying > 0) {
            underlying_ref = c.last_underlying;
            break;
          }
        }
        let atm_strike = null;
        if (strikes.length > 0 && underlying_ref) {
          atm_strike = strikes.reduce((prev, curr) => Math.abs(curr - underlying_ref) < Math.abs(prev - underlying_ref) ? curr : prev);
        } else if (strikes.length > 0) {
          atm_strike = strikes[Math.floor(strikes.length / 2)];
        }
        return {
          symbol: sym,
          expiration: exp,
          strikes,
          atm_strike,
          underlying_ref,
          contracts
        };
      }

      if (endpoint === '/api/timeseries' || path.startsWith('/api/timeseries')) {
        const exp = params.expiration;
        const strike = parseFloat(params.strike);
        const right = (params.right || 'call').toLowerCase();
        const series = records
          .filter(r => r.expiration === exp && Math.abs(r.strike - strike) < 1e-4 && r.right.toLowerCase() === right)
          .sort((a, b) => a.trade_date.localeCompare(b.trade_date));
        return {
          symbol: sym,
          expiration: exp,
          strike,
          right,
          count: series.length,
          series
        };
      }

      if (endpoint === '/api/smile' || path.startsWith('/api/smile')) {
        const td = params.trade_date;
        const exp = params.expiration;
        const dateRows = records.filter(r => r.trade_date === td);
        const avail_exps = [...new Set(dateRows.map(r => r.expiration))].sort();
        let selected_exp = exp;
        if (!selected_exp || !avail_exps.includes(selected_exp)) {
          selected_exp = avail_exps.length > 0 ? avail_exps[0] : null;
        }
        const chain = dateRows
          .filter(r => !selected_exp || r.expiration === selected_exp)
          .sort((a, b) => a.strike - b.strike || a.right.localeCompare(b.right));
        let underlying_price = null;
        for (const r of chain) {
          if (r.underlying_price != null) {
            underlying_price = r.underlying_price;
            break;
          }
        }
        return {
          symbol: sym,
          trade_date: td,
          available_expirations: avail_exps,
          selected_expiration: selected_exp,
          underlying_price,
          chain
        };
      }

      if (endpoint === '/api/chain' || path.startsWith('/api/chain')) {
        const availDates = [...new Set(records.map(r => r.trade_date))].sort();
        const td_str = (params.trade_date && availDates.includes(params.trade_date)) ? params.trade_date : availDates[availDates.length - 1];
        const dayRows = records.filter(r => r.trade_date === td_str);
        
        let underlying_price = null;
        for (const r of dayRows) {
          if (r.underlying_price != null) {
            underlying_price = r.underlying_price;
            break;
          }
        }

        const expMap = {};
        for (const r of dayRows) {
          if (!expMap[r.expiration]) {
            expMap[r.expiration] = {
              expiration: r.expiration,
              dte: r.dte,
              contract_count: 0,
              total_volume: 0,
              total_oi: 0,
              calls_volume: 0,
              puts_volume: 0,
              calls_oi: 0,
              puts_oi: 0
            };
          }
          const item = expMap[r.expiration];
          item.contract_count += 1;
          const vol = r.volume || 0;
          const oi = r.open_interest || 0;
          item.total_volume += vol;
          item.total_oi += oi;
          if (r.right === 'call') {
            item.calls_volume += vol;
            item.calls_oi += oi;
          } else {
            item.puts_volume += vol;
            item.puts_oi += oi;
          }
        }
        const exp_records = Object.values(expMap).sort((a,b) => a.expiration.localeCompare(b.expiration));
        const avail_exps = exp_records.map(e => e.expiration);
        let selected_exp = params.expiration;
        if (!selected_exp || !avail_exps.includes(selected_exp)) {
          selected_exp = avail_exps.length > 0 ? avail_exps[0] : null;
        }

        const sub_records = selected_exp ? dayRows.filter(r => r.expiration === selected_exp) : [];
        const selected_dte = sub_records.length > 0 ? sub_records[0].dte : null;

        const strikes_map = {};
        for (const r of sub_records) {
          const st = r.strike;
          if (!strikes_map[st]) {
            let moneyness = null;
            if (underlying_price && underlying_price > 0) {
              moneyness = Number((((st - underlying_price) / underlying_price) * 100).toFixed(2));
            }
            strikes_map[st] = {
              strike: st,
              is_atm: false,
              moneyness_pct: moneyness,
              call: null,
              put: null
            };
          }
          if (r.right === 'call' || r.right === 'put') {
            strikes_map[st][r.right] = r;
          }
        }

        const sorted_strikes = Object.keys(strikes_map).map(Number).sort((a,b) => a - b);
        if (sorted_strikes.length > 0 && underlying_price != null) {
          const atm_st = sorted_strikes.reduce((prev, curr) => Math.abs(curr - underlying_price) < Math.abs(prev - underlying_price) ? curr : prev);
          if (strikes_map[atm_st]) {
            strikes_map[atm_st].is_atm = true;
          }
        }
        const strikes_list = sorted_strikes.map(st => strikes_map[st]);

        const session_c_vol = exp_records.reduce((sum, e) => sum + e.calls_volume, 0);
        const session_p_vol = exp_records.reduce((sum, e) => sum + e.puts_volume, 0);
        const session_c_oi = exp_records.reduce((sum, e) => sum + e.calls_oi, 0);
        const session_p_oi = exp_records.reduce((sum, e) => sum + e.puts_oi, 0);

        const exp_c_vol = strikes_list.reduce((sum, s) => sum + (s.call && s.call.volume ? s.call.volume : 0), 0);
        const exp_p_vol = strikes_list.reduce((sum, s) => sum + (s.put && s.put.volume ? s.put.volume : 0), 0);
        const exp_c_oi = strikes_list.reduce((sum, s) => sum + (s.call && s.call.open_interest ? s.call.open_interest : 0), 0);
        const exp_p_oi = strikes_list.reduce((sum, s) => sum + (s.put && s.put.open_interest ? s.put.open_interest : 0), 0);

        const totals = {
          session_call_volume: session_c_vol,
          session_put_volume: session_p_vol,
          session_total_volume: session_c_vol + session_p_vol,
          session_call_oi: session_c_oi,
          session_put_oi: session_p_oi,
          session_total_oi: session_c_oi + session_p_oi,
          session_put_call_volume_ratio: session_c_vol > 0 ? Number((session_p_vol / session_c_vol).toFixed(3)) : null,
          session_put_call_oi_ratio: session_c_oi > 0 ? Number((session_p_oi / session_c_oi).toFixed(3)) : null,
          active_exp_call_volume: exp_c_vol,
          active_exp_put_volume: exp_p_vol,
          active_exp_total_volume: exp_c_vol + exp_p_vol,
          active_exp_call_oi: exp_c_oi,
          active_exp_put_oi: exp_p_oi,
          active_exp_total_oi: exp_c_oi + exp_p_oi
        };

        return {
          symbol: sym,
          trade_date: td_str,
          available_trade_dates: availDates,
          underlying_price,
          expirations: exp_records,
          selected_expiration: selected_exp,
          selected_dte,
          strikes: strikes_list,
          totals
        };
      }

      if (endpoint === '/api/gdrive-pull' || path.startsWith('/api/gdrive-pull')) {
        return {
          symbol: sym,
          pulled_years: [2026],
          local_files: [`${sym}_2026.parquet`],
          note: 'Static demo mode: all mock data is bundled locally.'
        };
      }

      throw new Error(`Unsupported demo endpoint: ${path}`);
    }
"""


def export_docs_html() -> None:
    """Export docs/index.html adapted for static GitHub Pages with mock data."""
    with open(SRC_VIEWER_PY, "r", encoding="utf-8") as f:
        content = f.read()

    match = re.search(r'INDEX_HTML\s*=\s*r?"""(.*?)"""', content, re.DOTALL)
    if not match:
        raise RuntimeError("Could not find INDEX_HTML in src/options_data/viewer.py")

    html = match.group(1)

    # 1. Inject Disclaimer CSS into <style>
    html = html.replace("</style>", f"{DISCLAIMER_CSS}\n  </style>", 1)

    # 2. Inject Disclaimer Banner immediately after <body>
    html = html.replace("<body>", f"<body>\n{DISCLAIMER_BANNER_HTML}", 1)

    # 3. Update <header> branding & badges
    old_header_badges = """      <span class="badge">Polars Lazy Engine</span>
      <span class="badge badge-green" id="cache-badge">Local + GDrive Hybrid</span>"""
    new_header_badges = """      <span class="badge badge-amber">⚠️ 100% Mock / Synthetic Data</span>
      <span class="badge badge-green" id="cache-badge">GitHub Pages Static Demo</span>"""
    if old_header_badges in html:
        html = html.replace(old_header_badges, new_header_badges, 1)

    # 4. Replace GDrive pull button with repo action button in header
    old_status_bar = """      <span id="status-msg">Ready</span>
      <button class="secondary" onclick="pullFromGDrive()" title="Pull missing Parquet files for this ticker from Google Drive">☁ Pull Ticker from Google Drive</button>"""
    new_status_bar = """      <span id="status-msg">Ready (Demo Mode)</span>
      <button class="secondary" onclick="alert('This is a static GitHub Pages demo running on simulated Black-Scholes mock data. To download real options data, clone the repo and run locally.')" title="Demo Mode Info">ℹ️ Mock Mode Info</button>"""
    if old_status_bar in html:
        html = html.replace(old_status_bar, new_status_bar, 1)

    # 5. Inject Footer before </body>
    html = html.replace("</body>", f"{FOOTER_HTML}\n</body>", 1)

    # 6. Replace `async function apiGet(path)` with IN_BROWSER_ROUTER_JS
    old_api_get = """    async function apiGet(path) {
      const res = await fetch(path);
      if (!res.ok) {
        const err = await res.text();
        throw new Error(err || res.statusText);
      }
      return await res.json();
    }"""
    if old_api_get in html:
        html = html.replace(old_api_get, IN_BROWSER_ROUTER_JS, 1)

    out_file = DOCS_DIR / "index.html"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  ✓ Exported static HTML: {out_file} ({out_file.stat().st_size / 1024:.1f} KB)")


def main() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    SAMPLE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating synthetic mock options data...")
    start_date = datetime.date(2026, 2, 17)
    expiration_date = datetime.date(2026, 3, 20)

    symbols_config = [
        {
            "symbol": "AAPL",
            "spot": 228.0,
            "strikes": [210.0, 215.0, 220.0, 225.0, 230.0, 235.0, 240.0, 245.0, 250.0],
            "iv": 0.22,
        },
        {
            "symbol": "MSFT",
            "spot": 418.0,
            "strikes": [395.0, 400.0, 405.0, 410.0, 415.0, 420.0, 425.0, 430.0, 435.0, 440.0],
            "iv": 0.24,
        },
        {
            "symbol": "NVDA",
            "spot": 138.0,
            "strikes": [120.0, 125.0, 130.0, 135.0, 140.0, 145.0, 150.0, 155.0, 160.0],
            "iv": 0.42,
        },
    ]

    symbols_list = build_symbols_metadata()
    symbols_file = SAMPLE_DATA_DIR / "symbols.json"
    with open(symbols_file, "w", encoding="utf-8") as f:
        json.dump({"symbols": symbols_list}, f, indent=2)
    print(f"  ✓ Saved symbols registry: {symbols_file}")

    for cfg in symbols_config:
        sym = cfg["symbol"]
        records = generate_synthetic_symbol_data(
            symbol=sym,
            base_spot=cfg["spot"],
            strikes=cfg["strikes"],
            base_iv=cfg["iv"],
            start_date=start_date,
            expiration_date=expiration_date,
        )
        sym_file = SAMPLE_DATA_DIR / f"{sym}.json"
        with open(sym_file, "w", encoding="utf-8") as f:
            json.dump(records, f, separators=(",", ":"))
        file_size_kb = sym_file.stat().st_size / 1024
        print(f"  ✓ Saved {sym} ({len(records)} records, {file_size_kb:.1f} KB): {sym_file}")

    print("Exporting docs/index.html with static mock client and disclaimers...")
    export_docs_html()
    print("Static GitHub Pages demo preparation complete!")


if __name__ == "__main__":
    main()
