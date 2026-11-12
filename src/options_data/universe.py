"""S&P 500 Universe loader and Google Sheet exporter."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Literal

import polars as pl
from dotenv import load_dotenv
from rich.console import Console

from options_data.gdrive_sync import get_gdrive_base_cmd

load_dotenv()
console = Console()

# Optional Google Sheet ID for S&P 500 universe export (configured via SP500_UNIVERSE_SHEET_ID in .env)
DEFAULT_UNIVERSE_SHEET_ID = os.getenv("SP500_UNIVERSE_SHEET_ID", "")
DEFAULT_UNIVERSE_CSV = Path("config/sp500_universe.csv")
DEFAULT_RAW_UNIVERSE_CSV = Path("config/sp500_universe_raw.csv")
DEFAULT_EXCLUDED_CSV = Path("config/sp500_excluded.csv")

UniverseFilter = Literal["active", "current", "removed_active", "all"]


def export_universe_sheet_from_gdrive(
    sheet_id: str | None = None,
    raw_csv_path: Path = DEFAULT_RAW_UNIVERSE_CSV,
    clean_csv_path: Path = DEFAULT_UNIVERSE_CSV,
    excluded_csv_path: Path = DEFAULT_EXCLUDED_CSV,
) -> pl.DataFrame:
    """Export the S&P 500 Google Sheet from Drive to CSV and split into clean active & excluded CSVs."""
    effective_sheet_id = (sheet_id or os.getenv("SP500_UNIVERSE_SHEET_ID", "")).strip()
    if not effective_sheet_id:
        raise ValueError(
            "No Google Sheet ID provided. Specify --sheet-id or set SP500_UNIVERSE_SHEET_ID in your environment."
        )
    base_cmd, env = get_gdrive_base_cmd()
    raw_csv_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = base_cmd + ["readonly", "export", effective_sheet_id, str(raw_csv_path), "--format", "csv"]
    res = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    if res.returncode != 0:
        err = (res.stderr or res.stdout).strip()
        raise RuntimeError(f"Failed to export Google Sheet {effective_sheet_id}: {err}")

    return clean_and_save_universe_csv(
        raw_csv_path=raw_csv_path,
        clean_csv_path=clean_csv_path,
        excluded_csv_path=excluded_csv_path,
    )


def clean_and_save_universe_csv(
    raw_csv_path: Path = DEFAULT_RAW_UNIVERSE_CSV,
    clean_csv_path: Path = DEFAULT_UNIVERSE_CSV,
    excluded_csv_path: Path = DEFAULT_EXCLUDED_CSV,
) -> pl.DataFrame:
    """Parse the raw exported Google Sheet CSV into clean active (575) and excluded (51) CSV files."""
    df = pl.read_csv(raw_csv_path, ignore_errors=True)
    df = df.rename(
        {
            "Current Ticker": "ticker",
            "Historical Ticker (if changed)": "historical_ticker",
            "Company Name": "company_name",
            "S&P 500 Status": "sp500_status",
            "Current Index / Trading Status": "trading_status",
            "Date Added / Removed": "date_added_or_removed",
            "Notes": "notes",
            "Source URL": "source_url",
        }
    )

    active_df = df.filter(
        pl.col("sp500_status").is_in(["Current S&P 500", "Removed (Still Actively Trading)"])
    ).sort("ticker")

    excluded_df = df.filter(pl.col("sp500_status").str.starts_with("Excluded")).sort("ticker")

    clean_csv_path.parent.mkdir(parents=True, exist_ok=True)
    active_df.write_csv(clean_csv_path)
    excluded_df.write_csv(excluded_csv_path)

    console.print(
        f"[green]✓ Universe CSV ready:[/green] {active_df.height} active stocks -> [bold]{clean_csv_path}[/bold] "
        f"({excluded_df.height} excluded -> [bold]{excluded_csv_path}[/bold])"
    )
    return active_df


def load_universe_symbols(
    csv_path: Path = DEFAULT_UNIVERSE_CSV,
    *,
    mode: UniverseFilter = "active",
    include_historical_tickers: bool = False,
    sheet_id: str | None = None,
    excluded_csv_path: Path = DEFAULT_EXCLUDED_CSV,
) -> list[str]:
    """Load ticker symbols from the S&P 500 Universe CSV.

    Parameters:
    - `mode`:
      - `'active'` (default): All 575 actively trading stocks (503 Current S&P 500 + 72 Removed but Active)
      - `'current'`: Only the 503 Current S&P 500 constituents
      - `'removed_active'`: Only the 72 Removed (Still Actively Trading) stocks
      - `'all'`: All 575 active stocks + 51 excluded/delisted stocks
    - `include_historical_tickers`: If True, also includes pre-rename tickers (e.g. ADS, HFC, FBHS, GPS, WLTW).
    """
    if not csv_path.exists():
        if DEFAULT_RAW_UNIVERSE_CSV.exists():
            clean_and_save_universe_csv(
                raw_csv_path=DEFAULT_RAW_UNIVERSE_CSV,
                clean_csv_path=csv_path,
                excluded_csv_path=excluded_csv_path,
            )
        else:
            export_universe_sheet_from_gdrive(
                sheet_id=sheet_id,
                clean_csv_path=csv_path,
                excluded_csv_path=excluded_csv_path,
            )

    df = pl.read_csv(csv_path)

    # Support both normalized ('ticker', 'sp500_status') and raw header names
    ticker_col = "ticker" if "ticker" in df.columns else "Current Ticker"
    hist_col = (
        "historical_ticker"
        if "historical_ticker" in df.columns
        else "Historical Ticker (if changed)"
    )
    status_col = "sp500_status" if "sp500_status" in df.columns else "S&P 500 Status"

    if mode == "current":
        df = df.filter(pl.col(status_col) == "Current S&P 500")
    elif mode == "removed_active":
        df = df.filter(pl.col(status_col) == "Removed (Still Actively Trading)")
    elif mode == "active":
        df = df.filter(
            pl.col(status_col).is_in(["Current S&P 500", "Removed (Still Actively Trading)"])
        )

    symbols: list[str] = []
    seen: set[str] = set()

    for row in df.iter_rows(named=True):
        t = (row.get(ticker_col) or "").strip().upper()
        if t and t not in seen:
            seen.add(t)
            symbols.append(t)
        if include_historical_tickers:
            ht = (row.get(hist_col) or "").strip().upper()
            if ht and ht != "DELISTED" and ht not in seen:
                seen.add(ht)
                symbols.append(ht)

    if mode == "all" and excluded_csv_path.exists():
        ex_df = pl.read_csv(excluded_csv_path)
        ex_ticker_col = "ticker" if "ticker" in ex_df.columns else "Current Ticker"
        for row in ex_df.iter_rows(named=True):
            t = (row.get(ex_ticker_col) or "").strip().upper()
            if t and t not in seen:
                seen.add(t)
                symbols.append(t)

    return symbols
