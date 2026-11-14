# ThetaData Options Pipeline

[![CI](https://github.com/frankliuao/thetadata_options/actions/workflows/ci.yml/badge.svg)](https://github.com/frankliuao/thetadata_options/actions)
[![Python Version](https://img.shields.io/badge/python-3.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![Storage](https://img.shields.io/badge/storage-Apache%20Parquet%20(ZSTD)-orange.svg)](https://parquet.apache.org/)
[![Engine](https://img.shields.io/badge/engine-Polars-teal.svg)](https://pola.rs/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A production-grade, fault-tolerant Python ETL pipeline and interactive analytics suite for downloading, normalizing, and archiving multi-year US options chains, Black-Scholes/Binomial Greeks, Open Interest, and underlying prices from **ThetaData v3** into partitioned, high-compression **Apache Parquet** lakehouses with hybrid Google Drive cloud sync.

Designed specifically for quantitative researchers, algorithmic traders, and financial engineers who require clean, standardized, point-in-time options data for backtesting options strategies (cash-secured puts, covered calls, calendar spreads, dispersion, volatility trading) without suffering from vendor SDK timeouts, silent data dropouts, or unscaled Greeks.

---

## 🌟 Key Highlights & Engineering Features

```
┌─────────────────┐       ┌──────────────────────┐       ┌────────────────────────┐
│  ThetaData v3   │ ────> │  Polars Normalizer   │ ────> │  Atomic Parquet Lake   │
│  gRPC / REST    │       │  & Math Scaler       │       │  (ZSTD / Partitioned)  │
└─────────────────┘       └──────────────────────┘       └────────────────────────┘
         │                           │                                │
  Auto-Reauth Lock           Greeks Standardization          Optional Cloud Sync
 (15m Token Refresh)        (Vega/Rho 1%, Prior OI)          (Google Drive Mutex)
```

- **⚡ Zero-Drop Session Persistence**: Proactive 8-minute token renewal and thread-safe dynamic re-authentication completely bypasses ThetaData's ~15-minute gRPC session token TTL, enabling seamless multi-day backfills without `UNAUTHENTICATED` disconnections.
- **📐 Quantitative Greeks Standardization**: Continuous Black-Scholes/Binomial Vega & Rho derivatives ($\partial V / \partial \sigma$ and $\partial V / \partial r$) are scaled to standard financial market convention (sensitivity per 1 percentage point move) via `vega_per_pct` and `rho_per_pct`, while preserving raw vendor values.
- **📈 Standardized Black-Scholes Baseline (`--greeks-model bs`, Default)**: Default Greeks use the industry-standard Black-Scholes EOD model calibrated against the SOFR risk-free yield curve and OPRA NBBO midpoints. The American Binomial Tree model (`--greeks-model binomial`, 101 steps) is also available on demand for discrete dividend and early-exercise pricing.
- **🏷️ Dual-Class OCC Symbology Translation**: Automatically converts dual-class equity tickers to official OCC option roots (e.g. `BRK.B` → `BRKB`, `BF.B` → `BFB`) across all options endpoints, preventing missing chains on S&P 500 constituents.
- **📊 Non-Lookahead Open Interest Alignment**: Daily OPRA open interest prints (published at ~06:30 ET) are joined to the active trade session and explicitly documented as `open_interest_prior_close` to guarantee point-in-time backtest integrity.
- **🛡️ Null-Preserving Deduplication & Atomic Writes**: Stacking daily chains into yearly archives uses `group_by.drop_nulls().last()`, ensuring partial raw updates never erase existing open interest. Writes use `.parquet.tmp` atomic renaming to eliminate corrupted files.
- **🔄 Fault-Tolerant Catch-up Updater (`update-options`)**: Automatically detects the latest downloaded trading day per symbol, synchronizes underlying stock EOD data, iterates missing sessions across symbols in parallel, heals internal gaps, and halts on failed days so subsequent runs resume cleanly without historical holes.
- **☁️ Cloud Backup & Mutex Sync**: Integrates with the Google Drive CLI (`gdrive`) with full mutex lock protection to ensure multi-threaded jobs never race on folder creation or in-place file updates.
- **📈 Interactive TradingView-Style Web Dashboard (`options-viewer`)**: Local web application featuring synchronized charts, continuous modifier-held wheel zoom (`Ctrl`/`⌘` + scroll), axis-drag price/timeline scaling, 1-click ATM/Max-OI selection, cross-sectional IV smiles, and 1-click CSV export.

---

## 🏛️ S&P 500 Universe Structure

The repository bundles a curated, multi-tier universe of S&P 500 constituents under `config/`:

| File | Count | Description |
| :--- | :---: | :--- |
| **`config/sp500_universe.csv`** | **573** | **Active Universe (Default)**: 503 Current S&P 500 + 70 Removed still actively trading. |
| **`config/sp500_active.csv`** | 503 | Current active S&P 500 index constituents. |
| **`config/sp500_removed.csv`** | 70 | Historical constituents removed from the S&P 500 that remain actively trading. |
| **`config/sp500_excluded.csv`** | 54 | Excluded, delisted, or acquired companies (e.g. `DAY`, `CDAY`, `TWTR`). |
| **`config/sp500_universe_3tabs.xlsx`** | Master | Excel 3-tab workbook from which CSVs are exported. |

Use `--universe {active,current,removed_active,all}` in CLI commands to select your desired constituent universe. Pass `--include-historical-tickers` to include pre-rename tickers (e.g. `GPS`, `FBHS`, `ADS`).

---

## 📦 Installation & Setup

### Prerequisites
- Python **3.12+** (Tested on Python 3.12 and 3.13)
- [Poetry](https://python-poetry.org/) (recommended) or `pip`
- A valid [ThetaData](https://thetadata.net/) subscription (**Standard** or **Pro** tier required for historical Greeks and Open Interest; Value tier only provides basic quotes)

### 1. Clone Repository & Install Dependencies

```bash
git clone https://github.com/frankliuao/thetadata_options.git
cd thetadata_options

# Install dependencies using Poetry:
poetry install

# Or install in editable mode with pip:
pip install -e .
```

### 2. Configure Environment

Copy the example environment file and configure your credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```ini
# ThetaData v3 API Key (Obtain from https://thetadata.net user portal)
THETADATA_API_KEY="your_thetadata_api_key_here"

# Concurrency limit (Standard Tier = 4, Pro Tier = 8, Value Tier = 2)
THETADATA_MAX_WORKERS=4

# Optional: Google Drive folder ID for automated cloud backup (leave blank to disable cloud sync)
GDRIVE_FOLDER_ID=""

# Optional: Google Sheet ID if refreshing universe from Google Sheets
SP500_UNIVERSE_SHEET_ID=""
```

### 3. Verify Setup Offline (Zero Credentials Needed)

Verify your Polars environment and Parquet round-trip without making any live network calls or using API credentials:

```bash
poetry run download-options --mock-test
```

### 4. Verify ThetaData Credentials (Optional)

Verify your ThetaData subscription tier and gRPC connection:

```bash
poetry run download-options --verify-auth
```

---

## 🚀 Usage & CLI Commands

### 1. Download Historical Options (`download-options`)

Download daily EOD options, Greeks, and open interest for specific tickers or the full universe:

```bash
# Download 2020–2024 history for single or multiple symbols:
poetry run download-options --symbols SPY QQQ AAPL --start-date 2020-01-01 --end-date 2024-12-31 --workers 4

# Download full active S&P 500 universe with storage-optimized filters (ATM ±50 strikes, DTE <= 150):
poetry run download-options --universe active --start-date 2016-01-01 --strike-range 50 --max-dte 150 --workers 4

# Test downloading the first 5 symbols of the universe:
poetry run download-options --universe active --limit 5 --start-date 2024-01-01

# Inspect the resolved symbol list without downloading:
poetry run download-options --universe active --list-universe
```

#### Key `download-options` Arguments:
| Argument | Default | Description |
| :--- | :--- | :--- |
| `-s, --symbols` | `None` | Explicit ticker symbols (e.g. `--symbols SPY QQQ`). Overrides `--universe`. |
| `-u, --universe` | `active` | Universe slice: `active` (573), `current` (503), `removed_active` (70), `all` (627+). |
| `--start-date` | `2016-01-01` | Inclusive start date (`YYYY-MM-DD`). |
| `--end-date` | NYSE Today | Inclusive end date (`YYYY-MM-DD`). Defaults to current date in `America/New_York`. |
| `--greeks-model` | `bs` | Greeks pricing model: `bs` (Black-Scholes), `binomial` (American tree), `none`. |
| `--strike-range` | `50` | Number of strikes fetched above and below ATM. |
| `--max-dte` | `150` | Maximum Days-to-Expiration filter. |
| `-w, --workers` | `4` | Number of concurrent request worker threads. |
| `--limit`, `--offset`| `None`, `0` | Slice the resolved symbol list for batching or trial runs. |
| `--data-dir` | `./data` | Local Parquet lakehouse destination directory. |
| `--no-gdrive` | `False` | Disable automatic Google Drive cloud backup. |
| `--no-underlying` | `False` | Skip downloading underlying stock/index EOD data. |
| `--no-open-interest` | `False` | Skip daily Open Interest join. |
| `--mock-test` | `False` | Run offline synthetic verification without credentials. |
| `--verify-auth` | `False` | Authenticate with ThetaData, print tier info, and exit. |

---

### 2. Incremental Catch-up / Daily Updating (`update-options`)

Run daily after market close (17:15 America/New_York) to automatically detect missing sessions, download only missing recent days, synchronize underlying prices, and heal any internal gaps:

```bash
# Daily catch-up for all active universe symbols using default Black-Scholes Greeks:
poetry run update-options --workers 4

# Catch up specific tickers:
poetry run update-options --symbols SPY QQQ AAPL NVDA --workers 4

# Force catch-up starting from a specific date, ignoring auto-detection:
poetry run update-options --symbols SPY --since 2024-06-01

# Run with American Binomial Tree Greeks model:
poetry run update-options --symbols SPY AAPL --greeks-model binomial
```

#### Key `update-options` Arguments:
| Argument | Default | Description |
| :--- | :--- | :--- |
| `-s, --symbols` | Universe | Specific symbols to update (defaults to all active universe symbols). |
| `--since` | Auto | Force start date (`YYYY-MM-DD`), bypassing auto-detection. |
| `--end-date` | NYSE Today | Target end date (`YYYY-MM-DD`). |
| `--greeks-model` | `bs` | Greeks model: `bs`, `binomial`, or `none`. |
| `--strike-range` | `50` | Strikes above/below ATM. |
| `--max-dte` | `150` | Max Days-to-Expiration filter. |
| `--skip-underlying`| `False` | Skip downloading/updating underlying stock daily EOD data. |
| `--no-gdrive` | `False` | Skip uploading updated yearly Parquet files to Google Drive. |
| `-w, --workers` | `4` | Concurrent request worker threads. |

---

### 3. Interactive Web Dashboard (`options-viewer`)

Launch the local interactive web dashboard to visualize contract lifecycles, implied volatility smiles, and Greeks cross-sections:

```bash
# Launch dashboard and automatically open browser:
poetry run options-viewer --port 8050 --open-browser

# Launch on all interfaces (useful for remote servers or LAN access):
poetry run options-viewer --host 0.0.0.0 --port 8050

# Or run via script:
python scripts/run_viewer.py --port 8050
```

Navigate to **http://127.0.0.1:8050**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  ThetaData Options & Greeks Interactive Dashboard                           │
│  [ Symbol: Search AAPL / Apple... ▼ ] [ Date: ◀ 2026-09-15 ▶ ] [ ⚡ Latest ]  │
│  [ Expiration: 2026-09-21 (6d) ▼ ] [ Strike Filter: ±25% Active ▼ ]         │
├─────────────────────────────────────────────────────────────────────────────┤
│  Tabs: [ 📅 Daily Option Chain ] [ 📈 Contract Lifecycle ] [ 🌐 IV Smile ]   │
│                                                                             │
│  Dual Option Chain: Calls (Delta, IV, Bid/Ask, Mid) | Strike | Puts         │
│  • Click any contract's [📈 View] to jump directly to Contract Lifecycle    │
│  • From Contract Lifecycle, click [📅 View Chain] on any date to jump back │
│                                                                             │
│  💡 TradingView Interactivity:                                              │
│     • Click & drag chart body to Pan                                        │
│     • Hold Ctrl / ⌘ + scroll to Zoom (Smooth)                               │
│     • Normal scroll navigates page                                          │
│     • Drag axes horizontally/vertically to Stretch/Compress                 │
│     • Double-click to Auto-fit                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Dashboard Features:
- **📅 Daily Option Chain (Snapshot by Date)**: Inspect all available options across every strike and expiration for any trading day:
  - **Market Snapshot KPIs**: Underlying spot price, ATM reference strike, total available expirations, total day option volume, open interest, and Put/Call volume/OI ratios.
  - **Expirations Carousel**: Quick-switch pills for all trading expirations with DTE badges (e.g. `1d`, `3d`, `6d`, `31d`, `120d`).
  - **Dual-Sided Option Chain (Calls | Strike | Puts)**: Side-by-side terminal layout with Delta, IV, Bid, Ask, Mid, Volume, and Open Interest.
  - **In-The-Money (ITM) Highlighting**: Clear visual distinction for ITM vs. OTM options and prominent `★ ATM` badge.
  - **Bidirectional Navigation**: Click any Call or Put row to jump directly into the full multi-year **Contract Lifecycle** view for that exact contract, with a 1-click **⬅ Return to Chain** breadcrumb.
- **📈 Contract Lifecycle View**: Synchronized interactive charts tracking an option over its entire lifespan:
  - Option Price (`Mid`, `Bid`, `Ask`) vs. Underlying Spot & Strike
  - 1st-Order Greeks: `Delta` & `Gamma`, `Theta`, `Vega` (per 1%), `Rho` (per 1%)
  - Liquidity & Volatility: `Implied Volatility`, `Volume`, and `Open Interest`
  - 2nd-Order Greeks: `Vanna`, `Charm`, `Vomma`
  - **Jump to Date**: 1-click dropdown and data table links to view any day's full cross-sectional option chain.
- **🌐 IV Smile & Strike Greeks Cross-Section**: Select any historical `trade_date` and `expiration` to view the cross-sectional **Implied Volatility Smile / Skew**, strike vs. Greek profiles, and the **Call vs. Put Open Interest Wall**.
- **📋 Data Table & CSV Export**: Inspect full daily tabular records with 1-click **CSV Download** and per-row `📅 Chain` jumps.
- **🔍 Searchable Combobox**: Real-time ticker and company name search with keyboard navigation (`↑`/`↓`/`Enter`) and local cache badges.
- **🎯 1-Click Selectors**: Instant **Select ATM** and **Select Max OI** buttons.
- **⚡ TradingView-Style Interactivity**: Drag chart canvas to pan, drag axes to scale price or timeline, double-click to auto-fit, and hold `Ctrl` / `⌘` while scrolling for gentle, smooth zoom (normal scrolling navigates the page smoothly).
- **☁️ Hybrid Cloud Fetch**: Click **`☁ Pull from Google Drive`** to download only the active symbol's Parquet file into your local cache on demand.

---

### 4. Auxiliary Scripts

- **`scripts/fetch_latest_prices.py`**: Fetch 5-day market closing prices and percentage changes for all S&P 500 universe stocks using `yfinance`:
  ```bash
  python scripts/fetch_latest_prices.py
  ```
- **`scripts/download_options_eod.py`**: Direct executable runner for `download-options`.
- **`scripts/update_options.py`**: Direct executable runner for `update-options`.
- **`scripts/run_viewer.py`**: Direct executable runner for `options-viewer`.

---

## ☁️ Google Drive & Hybrid Cloud Storage Workflows

The full 11-year (2016–2026) consolidated S&P 500 options archive is **~51 GB** across 5,892 yearly `.parquet` files (`symbol=<SYM>/<SYM>_<YEAR>.parquet`), where each single symbol-year file is only **5 MB – 15 MB**.

**You do NOT need to download all 51 GB locally**. Choose any of the following storage workflows:

### Workflow 1: Google Drive for Desktop — "Stream Files" Mode (Mac / Windows)
Zero CLI setup or API credentials required:
1. Install [Google Drive for Desktop](https://www.google.com/drive/download/) and sign in.
2. Under Preferences, choose **Stream files** (files stay in the cloud and consume **0 GB** of local disk until accessed).
3. Point `options-viewer` or `OptionsDataLoader` at your virtual Drive mount:
   ```bash
   poetry run options-viewer \
     --data-dir "$HOME/Library/CloudStorage/GoogleDrive-you@gmail.com/My Drive/Options Strategy" \
     --no-gdrive-fetch
   ```

### Workflow 2: `rclone mount` (Linux Servers / Headless Cloud VMs)
Mount your Google Drive folder as a virtual local directory with on-demand VFS read caching:
```bash
rclone config  # Configure remote named 'gdrive'
mkdir -p ./gdrive_mount
rclone mount gdrive:"Options Strategy/thetadata_parquet" ./gdrive_mount/processed --vfs-cache-mode writes --daemon

poetry run options-viewer --data-dir ./gdrive_mount --no-gdrive-fetch
```

### Workflow 3: Antigravity IDE / Managed `gdrive` CLI
When running inside an environment with the Google Drive plugin or standalone `gdrive` binary installed, `GoogleDriveSync` automatically reuses your authenticated session. Set `GDRIVE_FOLDER_ID` in `.env`, and `options-viewer` will download only requested symbol files (~10 MB) into `data/` on demand.

### Workflow 4: Pure Local Storage
If you do not use Google Drive, simply pass `--no-gdrive` to CLI commands and `--no-gdrive-fetch` to `options-viewer`. All data remains entirely local in `./data`.

---

## 🗄️ Storage Layout

All data is organized into partitioned, Zstandard-compressed Parquet files:

```
data/
├── raw/
│   ├── options_eod/
│   │   └── symbol=AAPL/
│   │       ├── year=2024/
│   │       │   ├── AAPL_2024-01-02.parquet
│   │       │   └── AAPL_2024-01-03.parquet
│   │       └── year=2025/
│   │           └── ...
│   └── underlying_eod/
│       ├── AAPL_eod.parquet
│       ├── SPY_eod.parquet
│       └── ...
└── processed/
    └── options_eod/
        └── symbol=AAPL/
            ├── AAPL_2023.parquet
            ├── AAPL_2024.parquet
            └── AAPL_2025.parquet
```

- **`data/raw/options_eod/`**: Daily raw option chains partitioned by `symbol=<SYM>/year=<YEAR>/`.
- **`data/raw/underlying_eod/`**: Consolidated daily EOD price history for underlying equities and indices.
- **`data/processed/options_eod/`**: Consolidated yearly files deduplicated on `(trade_date, expiration, strike, right)` with null-preserving aggregation.

---

## 📊 Data Schema & Dictionary

Each normalized option record contains 40+ standardized financial attributes:

| Column | Type | Description |
| :--- | :--- | :--- |
| `trade_date` | `Date` | Trade session date (`America/New_York`). |
| `symbol` | `Utf8` | Underlying equity ticker (e.g. `AAPL`, `BRK.B`). |
| `expiration` | `Date` | Option expiration date. |
| `dte` | `Int32` | Calendar days to expiration: `(expiration - trade_date).days`. |
| `strike` | `Float64` | Contract strike price in dollars. |
| `right` | `Utf8` | Contract type (`call` or `put`). |
| `underlying_price`| `Float64` | Underlying asset closing price on `trade_date`. |
| `open`, `high`, `low`, `close` | `Float64` | Option transaction prices for the session. |
| `bid`, `ask`, `mid`, `spread` | `Float64` | Closing NBBO quotes, midpoint `(bid+ask)/2`, and spread `ask-bid`. |
| `volume`, `count` | `Int64` | Contract trade volume and transaction count. |
| `open_interest` | `Int64` | Total open contracts. |
| `open_interest_prior_close` | `Int64` | Prior session's OPRA open interest known at market open on `trade_date`. |
| `implied_vol` | `Float64` | Implied volatility (annualized decimal, e.g. `0.225` = 22.5%). |
| `delta` | `Float64` | Option delta ($\partial V / \partial S$). |
| `gamma` | `Float64` | Option gamma ($\partial^2 V / \partial S^2$). |
| `theta` | `Float64` | Option 1-day theta ($\partial V / \partial t$). |
| `vega` | `Float64` | Raw mathematical derivative ($\partial V / \partial \sigma$). |
| `vega_per_pct` | `Float64` | **Standardized Vega**: dollar change per 1% move in IV (`vega / 100`). |
| `rho` | `Float64` | Raw mathematical derivative ($\partial V / \partial r$). |
| `rho_per_pct` | `Float64` | **Standardized Rho**: dollar change per 1% move in interest rate (`rho / 100`). |
| `vanna` | `Float64` | 2nd-order Greek: sensitivity of Delta to IV ($\partial \Delta / \partial \sigma$). |
| `charm` | `Float64` | 2nd-order Greek: Delta decay over time ($\partial \Delta / \partial t$). |
| `vomma` | `Float64` | 2nd-order Greek: sensitivity of Vega to IV ($\partial \text{Vega} / \partial \sigma$). |

---

## 💻 Python Querying & Backtesting API

### 1. Filter Chains with `OptionsDataLoader.query_chain()`

```python
from options_data.loader import OptionsDataLoader

loader = OptionsDataLoader(data_dir="data")

# Query 30-50 DTE Put options with delta between -0.35 and -0.15
chain = loader.query_chain(
    symbol="AAPL",
    trade_date="2024-06-03",
    right="put",
    min_dte=30,
    max_dte=50,
    min_delta=-0.35,
    max_delta=-0.15,
)

print(chain.select([
    "trade_date", "expiration", "dte", "strike", "underlying_price",
    "bid", "ask", "mid", "delta", "vega_per_pct", "open_interest_prior_close"
]))
```

> **💡 Absolute Delta Filtering**: You can also pass `min_abs_delta=0.15, max_abs_delta=0.35`, which applies seamlessly to both calls and puts.

### 2. High-Performance Lazy Queries with Polars

```python
import polars as pl
from options_data.loader import OptionsDataLoader

loader = OptionsDataLoader(data_dir="data")

# Scan multi-year Parquet files lazily without loading full dataset into memory:
lf = loader.scan_symbol("SPY", years=[2023, 2024])

# Compute monthly average ATM implied volatility:
atm_iv = (
    lf.filter((pl.col("dte") >= 25) & (pl.col("dte") <= 35))
    .filter(pl.col("delta").abs().sub(0.50).abs() < 0.05)
    .group_by(pl.col("trade_date").dt.truncate("1mo").alias("month"))
    .agg([
        pl.col("implied_vol").mean().alias("avg_atm_iv"),
        pl.col("underlying_price").last().alias("spot_close"),
    ])
    .sort("month")
    .collect()
)

print(atm_iv)
```

### 3. SQL Querying with Polars SQLContext (Zero Extra Dependencies)

```python
import polars as pl

ctx = pl.SQLContext()
ctx.register("options", pl.scan_parquet("data/processed/options_eod/symbol=AAPL/*.parquet"))

df = ctx.execute("""
    SELECT trade_date, expiration, strike, right, mid, delta, vega_per_pct, open_interest_prior_close
    FROM options
    WHERE trade_date = '2024-01-10'
      AND dte BETWEEN 25 AND 60
      AND strike BETWEEN 170 AND 190
    ORDER BY strike, right
""").collect()

print(df)
```

*(You can also query the Parquet files using DuckDB via `pip install duckdb`)*.

---

## 🧪 Testing

The test suite includes **32 offline unit tests** covering schema casting, mathematical Greek scaling, multi-threaded catch-up synchronization, error recovery, internal gap healing, dual-class symbology, and the visualization query engine without requiring ThetaData credentials:

```bash
# Run full unit test suite:
poetry run python -m unittest discover -s tests -v
```

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

## ⚠️ Disclaimer

*This toolkit is for research and educational purposes only. Historical options data and Greeks calculations are provided by ThetaData. Past performance is no guarantee of future trading results. This software is not affiliated with or endorsed by ThetaData LLC or the Options Clearing Corporation (OCC).*
