"""S&P 500 Constituent Universe Management."""

from __future__ import annotations

import csv
import datetime
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Literal

import polars as pl
from dotenv import load_dotenv
from rich.console import Console

from options_data.gdrive_sync import get_gdrive_base_cmd

load_dotenv()
console = Console()

# Optional Google Sheet ID for S&P 500 universe export (configured via SP500_UNIVERSE_SHEET_ID in .env)
DEFAULT_UNIVERSE_SHEET_ID = os.getenv("SP500_UNIVERSE_SHEET_ID", "")
DEFAULT_UNIVERSE_CSV = Path("config/sp500_universe.csv")
DEFAULT_ACTIVE_CSV = Path("config/sp500_active.csv")
DEFAULT_REMOVED_CSV = Path("config/sp500_removed.csv")
DEFAULT_RAW_UNIVERSE_CSV = Path("config/sp500_universe_raw.csv")

UNIVERSE_COLUMNS = [
    "Current Ticker",
    "CIK",
    "Historical Ticker (if changed)",
    "Company Name",
]
NORMALIZED_COLUMNS = UNIVERSE_COLUMNS

REMOVED_ACTIVE_STATUSES = ["Removed (Still Actively Trading)", "Removed from S&P 500"]

UniverseFilter = Literal["active", "current", "removed_active", "all"]


def _cell_str(value: Any) -> str:
    """Convert an Excel cell value to a stripped string, formatting dates as YYYY-MM-DD."""
    if value is None:
        return ""
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.strftime("%Y-%m-%d")
    return str(value).strip()


def _read_xlsx_tab_rows(wb: Any, sheet_name: str) -> list[dict[str, str]]:
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
    return pl.DataFrame(schema={col: pl.String for col in UNIVERSE_COLUMNS})


def export_universe_sheet_from_gdrive(
    sheet_id: str | None = None,
    raw_csv_path: Path | None = None,
    clean_csv_path: Path = DEFAULT_UNIVERSE_CSV,
    excluded_csv_path: Path | None = None,
    xlsx_path: Path | None = None,
    active_csv_path: Path = DEFAULT_ACTIVE_CSV,
    removed_csv_path: Path = DEFAULT_REMOVED_CSV,
) -> pl.DataFrame:
    """Export the S&P 500 Google Sheet from Drive as temporary XLSX and generate clean CSVs."""
    del raw_csv_path, excluded_csv_path, xlsx_path  # Retained in signature for backward compatibility.
    effective_sheet_id = (sheet_id or os.getenv("SP500_UNIVERSE_SHEET_ID", "")).strip()
    if not effective_sheet_id:
        raise ValueError(
            "No Google Sheet ID provided. Specify --sheet-id or set SP500_UNIVERSE_SHEET_ID in your environment."
        )
    base_cmd, env = get_gdrive_base_cmd()

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        temp_xlsx = Path(tmp.name)
    try:
        cmd = base_cmd + ["readonly", "export", effective_sheet_id, str(temp_xlsx), "--format", "xlsx"]
        res = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
        if res.returncode != 0:
            err = (res.stderr or res.stdout).strip()
            raise RuntimeError(f"Failed to export Google Sheet {effective_sheet_id}: {err}")

        return clean_and_save_universe_xlsx(
            xlsx_path=temp_xlsx,
            clean_csv_path=clean_csv_path,
            active_csv_path=active_csv_path,
            removed_csv_path=removed_csv_path,
        )
    finally:
        if temp_xlsx.exists():
            temp_xlsx.unlink()


def clean_and_save_universe_xlsx(
    xlsx_path: Path | str,
    clean_csv_path: Path | str = DEFAULT_UNIVERSE_CSV,
    active_csv_path: Path | str = DEFAULT_ACTIVE_CSV,
    removed_csv_path: Path | str = DEFAULT_REMOVED_CSV,
    excluded_csv_path: Path | str | None = None,
) -> pl.DataFrame:
    """Parse the S&P 500 Excel workbook into normalized active and removed CSVs."""
    del excluded_csv_path  # Delisted & excluded tab is excluded per project specification.
    try:
        import openpyxl
    except ImportError:
        raise ImportError(
            "openpyxl is required to read .xlsx files. "
            "Please install it or use the default bundled CSV."
        )

    xlsx_path = Path(xlsx_path)
    clean_csv_path = Path(clean_csv_path)
    active_csv_path = Path(active_csv_path)
    removed_csv_path = Path(removed_csv_path)

    wb = openpyxl.load_workbook(xlsx_path, data_only=True)

    required_sheets = ["S&P 500 Active", "Removed - Still Trading"]
    missing_sheets = [s for s in required_sheets if s not in wb.sheetnames]
    if missing_sheets:
        raise ValueError(
            f"Expected worksheet(s) {missing_sheets} in workbook; found {wb.sheetnames}. "
            "Aborting refresh to prevent overwriting universe with empty data."
        )

    def _extract_tab(sheet_name: str) -> pl.DataFrame:
        records = []
        for r in _read_xlsx_tab_rows(wb, sheet_name):
            ticker = r.get("Current Ticker", "").strip().upper()
            if not ticker:
                continue
            hist = (r.get("Historical Ticker (if changed)") or r.get("Historical Ticker") or "").strip().upper()
            records.append({
                "Current Ticker": ticker,
                "CIK": r.get("CIK", "").strip(),
                "Historical Ticker (if changed)": hist,
                "Company Name": (r.get("Company Name") or "").strip(),
            })
        return (
            pl.DataFrame(records).select(UNIVERSE_COLUMNS).sort("Current Ticker")
            if records
            else _empty_universe_df()
        )

    active_only_df = _extract_tab("S&P 500 Active")
    removed_only_df = _extract_tab("Removed - Still Trading")

    if active_only_df.is_empty():
        raise ValueError("Parsed 'S&P 500 Active' tab is empty; aborting overwrite to prevent data loss.")
    if removed_only_df.is_empty():
        raise ValueError("Parsed 'Removed - Still Trading' tab is empty; aborting overwrite to prevent data loss.")

    combined_active_df = pl.concat([active_only_df, removed_only_df]).sort("Current Ticker")

    for path in (clean_csv_path, active_csv_path, removed_csv_path):
        path.parent.mkdir(parents=True, exist_ok=True)

    combined_active_df.write_csv(clean_csv_path)
    active_only_df.write_csv(active_csv_path)
    removed_only_df.write_csv(removed_csv_path)

    console.print(
        f"[green]✓ Universe CSVs ready:[/green] {combined_active_df.height} active stocks "
        f"({active_only_df.height} current -> [bold]{active_csv_path}[/bold], "
        f"{removed_only_df.height} removed -> [bold]{removed_csv_path}[/bold]) "
        f"-> [bold]{clean_csv_path}[/bold]"
    )
    return combined_active_df


def clean_and_save_universe_csv(
    raw_csv_path: Path | str = DEFAULT_RAW_UNIVERSE_CSV,
    clean_csv_path: Path | str = DEFAULT_UNIVERSE_CSV,
    active_csv_path: Path | str = DEFAULT_ACTIVE_CSV,
    removed_csv_path: Path | str = DEFAULT_REMOVED_CSV,
    excluded_csv_path: Path | str | None = None,
) -> pl.DataFrame:
    """Parse a legacy raw CSV into normalized active CSV files."""
    del excluded_csv_path, active_csv_path, removed_csv_path
    raw_csv_path = Path(raw_csv_path)
    clean_csv_path = Path(clean_csv_path)
    df = pl.read_csv(raw_csv_path, ignore_errors=True)
    cols = [c for c in UNIVERSE_COLUMNS if c in df.columns]
    res_df = df.select(cols)
    clean_csv_path.parent.mkdir(parents=True, exist_ok=True)
    res_df.write_csv(clean_csv_path)
    return res_df


def load_universe_symbols(
    csv_path: Path | str = DEFAULT_UNIVERSE_CSV,
    *,
    mode: UniverseFilter = "active",
    include_historical_tickers: bool = False,
    sheet_id: str | None = None,
    active_csv_path: Path | str = DEFAULT_ACTIVE_CSV,
    removed_csv_path: Path | str = DEFAULT_REMOVED_CSV,
    excluded_csv_path: Path | str | None = None,
) -> list[str]:
    """Load ticker symbols from the S&P 500 Universe CSV.

    Parameters:
    - `mode`:
      - `'active'` (default): All 599 actively trading stocks (503 Current S&P 500 + 96 Removed but Active)
      - `'current'`: Only the 503 Current S&P 500 constituents
      - `'removed_active'`: Only the 96 Removed (Still Actively Trading) stocks
      - `'all'`: All 599 active stocks (and optional excluded stocks if excluded_csv_path exists)
    - `include_historical_tickers`: If True, also includes distinct pre-rename tickers (e.g. GPS, GAP, WLTW).
    """
    csv_path = Path(csv_path)
    active_csv_path = Path(active_csv_path)
    removed_csv_path = Path(removed_csv_path)

    if not csv_path.exists():
        if DEFAULT_RAW_UNIVERSE_CSV.exists():
            clean_and_save_universe_csv(
                raw_csv_path=DEFAULT_RAW_UNIVERSE_CSV,
                clean_csv_path=csv_path,
            )
        else:
            export_universe_sheet_from_gdrive(
                sheet_id=sheet_id,
                clean_csv_path=csv_path,
                active_csv_path=active_csv_path,
                removed_csv_path=removed_csv_path,
            )

    target_path = csv_path
    if csv_path == DEFAULT_UNIVERSE_CSV:
        if mode == "current" and active_csv_path.exists():
            target_path = active_csv_path
        elif mode == "removed_active" and removed_csv_path.exists():
            target_path = removed_csv_path

    df = pl.read_csv(target_path)

    ticker_col = (
        "Current Ticker"
        if "Current Ticker" in df.columns
        else ("ticker" if "ticker" in df.columns else df.columns[0])
    )
    hist_col = (
        "Historical Ticker (if changed)"
        if "Historical Ticker (if changed)" in df.columns
        else (
            "historical_ticker"
            if "historical_ticker" in df.columns
            else "Historical Ticker"
        )
    )

    if "sp500_status" in df.columns or "S&P 500 Status" in df.columns:
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

    if mode == "all" and excluded_csv_path and Path(excluded_csv_path).exists():
        ex_df = pl.read_csv(excluded_csv_path)
        ex_ticker_col = (
            "Current Ticker"
            if "Current Ticker" in ex_df.columns
            else ("ticker" if "ticker" in ex_df.columns else "Historical Ticker")
        )
        ex_hist_col = (
            "Historical Ticker (if changed)"
            if "Historical Ticker (if changed)" in ex_df.columns
            else ("historical_ticker" if "historical_ticker" in ex_df.columns else "Historical Ticker")
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
