"""S&P 500 Universe loader and Google Sheet exporter."""

from __future__ import annotations

import datetime
import os
import subprocess
from pathlib import Path
from typing import Any, Literal

import openpyxl
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
DEFAULT_UNIVERSE_XLSX = Path("config/sp500_universe_3tabs.xlsx")
DEFAULT_ACTIVE_CSV = Path("config/sp500_active.csv")
DEFAULT_REMOVED_CSV = Path("config/sp500_removed.csv")
DEFAULT_EXCLUDED_CSV = Path("config/sp500_excluded.csv")

NORMALIZED_COLUMNS = [
    "ticker",
    "historical_ticker",
    "company_name",
    "sp500_status",
    "trading_status",
    "date_added_or_removed",
    "notes",
    "source_url",
]
REMOVED_ACTIVE_STATUSES = ["Removed (Still Actively Trading)", "Removed from S&P 500"]

UniverseFilter = Literal["active", "current", "removed_active", "all"]


def _cell_str(value: Any) -> str:
    """Convert an Excel cell value to a stripped string, formatting dates as YYYY-MM-DD."""
    if value is None:
        return ""
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.strftime("%Y-%m-%d")
    return str(value).strip()


def _read_xlsx_tab_rows(wb: openpyxl.Workbook, sheet_name: str) -> list[dict[str, str]]:
    """Read a worksheet into a list of header-keyed string dictionaries."""
    if sheet_name not in wb.sheetnames:
        raise ValueError(f"Expected worksheet {sheet_name!r} in workbook; found {wb.sheetnames}")
    ws = wb[sheet_name]
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []
    headers = [_cell_str(h) for h in rows[0]]
    records: list[dict[str, str]] = []
    for raw_row in rows[1:]:
        if not any(cell is not None and _cell_str(cell) != "" for cell in raw_row):
            continue
        rec = {
            headers[i]: _cell_str(raw_row[i]) if i < len(raw_row) else ""
            for i in range(len(headers))
            if headers[i]
        }
        records.append(rec)
    return records


def _empty_universe_df() -> pl.DataFrame:
    return pl.DataFrame(schema={col: pl.String for col in NORMALIZED_COLUMNS})


def export_universe_sheet_from_gdrive(
    sheet_id: str | None = None,
    raw_csv_path: Path = DEFAULT_RAW_UNIVERSE_CSV,
    clean_csv_path: Path = DEFAULT_UNIVERSE_CSV,
    excluded_csv_path: Path = DEFAULT_EXCLUDED_CSV,
    xlsx_path: Path = DEFAULT_UNIVERSE_XLSX,
    active_csv_path: Path = DEFAULT_ACTIVE_CSV,
    removed_csv_path: Path = DEFAULT_REMOVED_CSV,
) -> pl.DataFrame:
    """Export the 3-tab S&P 500 Google Sheet from Drive as XLSX and generate normalized CSVs."""
    del raw_csv_path  # Retained in signature for backward compatibility.
    effective_sheet_id = (sheet_id or os.getenv("SP500_UNIVERSE_SHEET_ID", "")).strip()
    if not effective_sheet_id:
        raise ValueError(
            "No Google Sheet ID provided. Specify --sheet-id or set SP500_UNIVERSE_SHEET_ID in your environment."
        )
    base_cmd, env = get_gdrive_base_cmd()
    xlsx_path = Path(xlsx_path)
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = base_cmd + ["readonly", "export", effective_sheet_id, str(xlsx_path), "--format", "xlsx"]
    res = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    if res.returncode != 0:
        err = (res.stderr or res.stdout).strip()
        raise RuntimeError(f"Failed to export Google Sheet {effective_sheet_id}: {err}")

    return clean_and_save_universe_xlsx(
        xlsx_path=xlsx_path,
        clean_csv_path=clean_csv_path,
        active_csv_path=active_csv_path,
        removed_csv_path=removed_csv_path,
        excluded_csv_path=excluded_csv_path,
    )


def clean_and_save_universe_xlsx(
    xlsx_path: Path | str = DEFAULT_UNIVERSE_XLSX,
    clean_csv_path: Path | str = DEFAULT_UNIVERSE_CSV,
    active_csv_path: Path | str = DEFAULT_ACTIVE_CSV,
    removed_csv_path: Path | str = DEFAULT_REMOVED_CSV,
    excluded_csv_path: Path | str = DEFAULT_EXCLUDED_CSV,
) -> pl.DataFrame:
    """Parse the 3-tab S&P 500 Excel workbook into normalized active, removed, and excluded CSVs."""
    xlsx_path = Path(xlsx_path)
    clean_csv_path = Path(clean_csv_path)
    active_csv_path = Path(active_csv_path)
    removed_csv_path = Path(removed_csv_path)
    excluded_csv_path = Path(excluded_csv_path)

    wb = openpyxl.load_workbook(xlsx_path, data_only=True)

    tab1_records = [
        {
            "ticker": r.get("Current Ticker", ""),
            "historical_ticker": r.get("Historical Ticker (if changed)") or r.get("Historical Ticker", ""),
            "company_name": r.get("Company Name", ""),
            "sp500_status": r.get("S&P 500 Status") or "Current S&P 500",
            "trading_status": r.get("Current Index / Trading Status") or "S&P 500 (Active)",
            "date_added_or_removed": r.get("Date Added", ""),
            "notes": r.get("Notes", ""),
            "source_url": r.get("Source URL", ""),
        }
        for r in _read_xlsx_tab_rows(wb, "S&P 500 Active")
    ]

    tab2_records = [
        {
            "ticker": r.get("Current Ticker", ""),
            "historical_ticker": r.get("Historical Ticker (if changed)") or r.get("Historical Ticker", ""),
            "company_name": r.get("Company Name", ""),
            "sp500_status": "Removed (Still Actively Trading)",
            "trading_status": r.get("Current Trading Status") or "Removed (Active)",
            "date_added_or_removed": r.get("Date Removed from S&P 500", ""),
            "notes": r.get("Reason for Removal / Notes") or r.get("Notes", ""),
            "source_url": r.get("Source URL", ""),
        }
        for r in _read_xlsx_tab_rows(wb, "Removed - Still Trading")
    ]

    tab3_records = [
        {
            "ticker": r.get("Current Ticker (if changed)", ""),
            "historical_ticker": r.get("Historical Ticker", ""),
            "company_name": r.get("Company Name", ""),
            "sp500_status": r.get("Exclusion Category / Status", ""),
            "trading_status": r.get("Trading Status", ""),
            "date_added_or_removed": r.get("Date Delisted / Removed", ""),
            "notes": r.get("Reason for Removal / Notes") or r.get("Notes", ""),
            "source_url": r.get("Source URL", ""),
        }
        for r in _read_xlsx_tab_rows(wb, "Delisted & Excluded")
    ]

    active_only_df = (
        pl.DataFrame(tab1_records).select(NORMALIZED_COLUMNS).sort("ticker")
        if tab1_records
        else _empty_universe_df()
    )
    removed_only_df = (
        pl.DataFrame(tab2_records).select(NORMALIZED_COLUMNS).sort("ticker")
        if tab2_records
        else _empty_universe_df()
    )
    combined_active_df = pl.concat([active_only_df, removed_only_df]).sort("ticker")
    excluded_df = (
        pl.DataFrame(tab3_records).select(NORMALIZED_COLUMNS).sort("historical_ticker")
        if tab3_records
        else _empty_universe_df()
    )

    for path in (clean_csv_path, active_csv_path, removed_csv_path, excluded_csv_path):
        path.parent.mkdir(parents=True, exist_ok=True)

    combined_active_df.write_csv(clean_csv_path)
    active_only_df.write_csv(active_csv_path)
    removed_only_df.write_csv(removed_csv_path)
    excluded_df.write_csv(excluded_csv_path)

    console.print(
        f"[green]✓ Universe CSVs ready:[/green] {combined_active_df.height} active stocks "
        f"({active_only_df.height} current -> [bold]{active_csv_path}[/bold], "
        f"{removed_only_df.height} removed -> [bold]{removed_csv_path}[/bold]) "
        f"-> [bold]{clean_csv_path}[/bold]; {excluded_df.height} excluded -> [bold]{excluded_csv_path}[/bold]"
    )
    return combined_active_df


def clean_and_save_universe_csv(
    raw_csv_path: Path | str = DEFAULT_RAW_UNIVERSE_CSV,
    clean_csv_path: Path | str = DEFAULT_UNIVERSE_CSV,
    excluded_csv_path: Path | str = DEFAULT_EXCLUDED_CSV,
) -> pl.DataFrame:
    """Parse a legacy single-sheet raw exported CSV into normalized active and excluded CSV files."""
    raw_csv_path = Path(raw_csv_path)
    clean_csv_path = Path(clean_csv_path)
    excluded_csv_path = Path(excluded_csv_path)
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
        pl.col("sp500_status").is_in(["Current S&P 500", *REMOVED_ACTIVE_STATUSES])
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
    csv_path: Path | str = DEFAULT_UNIVERSE_CSV,
    *,
    mode: UniverseFilter = "active",
    include_historical_tickers: bool = False,
    sheet_id: str | None = None,
    excluded_csv_path: Path | str = DEFAULT_EXCLUDED_CSV,
) -> list[str]:
    """Load ticker symbols from the S&P 500 Universe CSV.

    Parameters:
    - `mode`:
      - `'active'` (default): All 573 actively trading stocks (503 Current S&P 500 + 70 Removed but Active)
      - `'current'`: Only the 503 Current S&P 500 constituents
      - `'removed_active'`: Only the 70 Removed (Still Actively Trading) stocks
      - `'all'`: All 573 active stocks + excluded/delisted stocks from `excluded_csv_path`
    - `include_historical_tickers`: If True, also includes distinct pre-rename tickers (e.g. ADS, HFC, FBHS, GPS, WLTW).
    """
    csv_path = Path(csv_path)
    excluded_csv_path = Path(excluded_csv_path)
    if not csv_path.exists():
        if DEFAULT_UNIVERSE_XLSX.exists():
            clean_and_save_universe_xlsx(
                xlsx_path=DEFAULT_UNIVERSE_XLSX,
                clean_csv_path=csv_path,
                excluded_csv_path=excluded_csv_path,
            )
        elif DEFAULT_RAW_UNIVERSE_CSV.exists():
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
        else (
            "Historical Ticker (if changed)"
            if "Historical Ticker (if changed)" in df.columns
            else "Historical Ticker"
        )
    )
    status_col = "sp500_status" if "sp500_status" in df.columns else "S&P 500 Status"

    if mode == "current":
        df = df.filter(pl.col(status_col) == "Current S&P 500")
    elif mode == "removed_active":
        df = df.filter(pl.col(status_col).is_in(REMOVED_ACTIVE_STATUSES))
    elif mode == "active":
        df = df.filter(
            pl.col(status_col).is_in(["Current S&P 500", *REMOVED_ACTIVE_STATUSES])
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
            if ht and ht != "DELISTED" and ht != t and ht not in seen:
                seen.add(ht)
                symbols.append(ht)

    if mode == "all" and excluded_csv_path.exists():
        ex_df = pl.read_csv(excluded_csv_path)
        ex_ticker_col = (
            "ticker"
            if "ticker" in ex_df.columns
            else (
                "Current Ticker (if changed)"
                if "Current Ticker (if changed)" in ex_df.columns
                else "Current Ticker"
            )
        )
        ex_hist_col = (
            "historical_ticker"
            if "historical_ticker" in ex_df.columns
            else (
                "Historical Ticker"
                if "Historical Ticker" in ex_df.columns
                else "Historical Ticker (if changed)"
            )
        )
        for row in ex_df.iter_rows(named=True):
            t = (row.get(ex_ticker_col) or "").strip().upper()
            ht = (row.get(ex_hist_col) or "").strip().upper()
            sym = t or (ht if ht != "DELISTED" else "")
            if sym and sym not in seen:
                seen.add(sym)
                symbols.append(sym)
            if include_historical_tickers and ht and ht != "DELISTED" and ht != sym and ht not in seen:
                seen.add(ht)
                symbols.append(ht)

    return symbols
