"""High-performance Polars LazyFrame loader for backtesting on downloaded Parquet options data."""

from __future__ import annotations

import datetime
import os
from pathlib import Path
from typing import Literal

import polars as pl

from options_data.gdrive_sync import DEFAULT_GDRIVE_FOLDER_ID, GoogleDriveSync


def _scan_parquet_files(files: list[Path]) -> pl.LazyFrame:
    """Scan one or multiple Parquet files, harmonizing schema variations across years with diagonal concatenation."""
    if not files:
        raise ValueError("Cannot scan an empty file list")
    if len(files) == 1:
        return pl.scan_parquet(str(files[0]))
    return pl.concat([pl.scan_parquet(str(p)) for p in files], how="diagonal_relaxed")


class OptionsDataLoader:
    """Lazy Parquet scanner for options strategy backtesting and visualization."""

    def __init__(
        self,
        data_dir: str | Path = "data",
        *,
        auto_fetch_gdrive: bool = False,
        gdrive_folder_id: str | None = None,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.auto_fetch_gdrive = auto_fetch_gdrive
        self.gdrive_folder_id = gdrive_folder_id or os.getenv("GDRIVE_FOLDER_ID", DEFAULT_GDRIVE_FOLDER_ID)
        self._gdrive: GoogleDriveSync | None = None

    @property
    def gdrive(self) -> GoogleDriveSync | None:
        if self._gdrive is None and self.gdrive_folder_id:
            self._gdrive = GoogleDriveSync(root_folder_id=self.gdrive_folder_id)
        return self._gdrive

    def ensure_local_parquets(
        self,
        symbol: str,
        *,
        years: list[int] | None = None,
    ) -> list[Path]:
        """Ensure Parquet files for `symbol` exist locally, pulling on-demand from Google Drive if missing."""
        sym = symbol.upper()
        processed_dir = self.data_dir / "processed" / "options_eod" / f"symbol={sym}"
        existing = sorted(processed_dir.glob("*.parquet")) if processed_dir.exists() else []
        if years is not None:
            existing_years = set()
            for p in existing:
                try:
                    yr = int(p.stem.split("_")[-1])
                    existing_years.add(yr)
                except ValueError:
                    pass
            missing_years = [y for y in years if y not in existing_years]
            if missing_years and self.auto_fetch_gdrive and self.gdrive is not None:
                try:
                    self.gdrive.download_symbol_parquets(sym, self.data_dir, years=missing_years)
                except Exception:
                    pass
        elif not existing and self.auto_fetch_gdrive and self.gdrive is not None:
            try:
                self.gdrive.download_symbol_parquets(sym, self.data_dir)
            except Exception:
                pass
        return sorted(processed_dir.glob("*.parquet")) if processed_dir.exists() else []

    def scan_symbol(
        self,
        symbol: str,
        *,
        use_consolidated: bool = True,
        years: list[int] | None = None,
    ) -> pl.LazyFrame:
        """Return a Polars LazyFrame scanning all Parquet files for `symbol`."""
        sym = symbol.upper()
        processed_dir = self.data_dir / "processed" / "options_eod" / f"symbol={sym}"
        raw_dir = self.data_dir / "raw" / "options_eod" / f"symbol={sym}"

        if self.auto_fetch_gdrive:
            self.ensure_local_parquets(sym, years=years)

        processed_files: list[Path] = []
        if use_consolidated and processed_dir.exists():
            all_proc = sorted(processed_dir.glob("*.parquet"))
            if years is not None:
                year_set = set(years)
                processed_files = [
                    p for p in all_proc
                    if p.stem.split("_")[-1].isdigit() and int(p.stem.split("_")[-1]) in year_set
                ]
            else:
                processed_files = all_proc

        raw_files: list[Path] = []
        if raw_dir.exists():
            raw_files = sorted(raw_dir.glob("year=*/*.parquet"))

        if not processed_files and not raw_files:
            raise FileNotFoundError(
                f"No Parquet files found for symbol '{sym}' in {processed_dir} or {raw_dir}."
            )

        if not processed_files:
            return _scan_parquet_files(raw_files)

        if not raw_files:
            return _scan_parquet_files(processed_files)

        # Both processed and raw files exist.
        # Find raw files not yet covered by consolidated yearly files.
        covered_dates: set[datetime.date] = set()
        for pf in processed_files:
            try:
                dates_df = (
                    pl.scan_parquet(str(pf))
                    .select(pl.col("trade_date").unique())
                    .collect()
                )
                for td in dates_df["trade_date"].to_list():
                    if isinstance(td, datetime.date):
                        covered_dates.add(td)
            except Exception:
                continue

        unconsolidated_raw: list[Path] = []
        for rf in raw_files:
            try:
                date_str = rf.stem.replace(f"{sym}_", "")
                rf_date = datetime.date.fromisoformat(date_str)
                if rf_date not in covered_dates:
                    unconsolidated_raw.append(rf)
            except Exception:
                unconsolidated_raw.append(rf)

        sources = [_scan_parquet_files(processed_files)]
        if unconsolidated_raw:
            sources.append(_scan_parquet_files(unconsolidated_raw))

        if len(sources) == 1:
            return sources[0]
        return pl.concat(sources, how="diagonal_relaxed")

    def query_chain(
        self,
        symbol: str,
        *,
        trade_date: datetime.date | str | None = None,
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        right: Literal["call", "put", "both"] = "both",
        min_dte: int | None = None,
        max_dte: int | None = None,
        min_delta: float | None = None,
        max_delta: float | None = None,
        min_abs_delta: float | None = None,
        max_abs_delta: float | None = None,
        min_bid: float | None = None,
    ) -> pl.DataFrame:
        """Query historical option chains with predicate pushdown into Parquet.

        Parameters:
        - `symbol`: Equity/Index ticker (e.g. "SPY", "AAPL").
        - `trade_date`: Single trading date (convenience shorthand for start_date=end_date=trade_date).
        - `start_date`: Inclusive start trade date.
        - `end_date`: Inclusive end trade date.
        - `right`: "call", "put", or "both".
        - `min_dte`: Minimum Days to Expiration.
        - `max_dte`: Maximum Days to Expiration.
        - `min_delta`: Signed delta lower bound (e.g. -0.35 for puts).
        - `max_delta`: Signed delta upper bound (e.g. -0.15 for puts).
        - `min_abs_delta`: Absolute delta lower bound (e.g. 0.25).
        - `max_abs_delta`: Absolute delta upper bound (e.g. 0.35).
        - `min_bid`: Minimum bid price filter.
        """
        lf = self.scan_symbol(symbol)

        if trade_date is not None:
            if isinstance(trade_date, str):
                trade_date = datetime.date.fromisoformat(trade_date)
            if start_date is None:
                start_date = trade_date
            if end_date is None:
                end_date = trade_date

        if isinstance(start_date, str):
            start_date = datetime.date.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.date.fromisoformat(end_date)

        if start_date is not None:
            lf = lf.filter(pl.col("trade_date") >= start_date)
        if end_date is not None:
            lf = lf.filter(pl.col("trade_date") <= end_date)
        if right in ("call", "put"):
            lf = lf.filter(pl.col("right") == right)
        if min_dte is not None:
            lf = lf.filter(pl.col("dte") >= min_dte)
        if max_dte is not None:
            lf = lf.filter(pl.col("dte") <= max_dte)
        if min_delta is not None:
            lf = lf.filter(pl.col("delta") >= min_delta)
        if max_delta is not None:
            lf = lf.filter(pl.col("delta") <= max_delta)
        if min_abs_delta is not None:
            lf = lf.filter(pl.col("delta").abs() >= min_abs_delta)
        if max_abs_delta is not None:
            lf = lf.filter(pl.col("delta").abs() <= max_abs_delta)
        if min_bid is not None:
            lf = lf.filter(pl.col("bid") >= min_bid)

        return lf.collect()
