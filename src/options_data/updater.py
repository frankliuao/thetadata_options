"""Incremental updater to catch up missing days between initial download and latest trading days."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import logging
import os
import sys
from pathlib import Path

import polars as pl
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from thetadata.errors import AuthenticationError

from options_data.downloader import (
    DownloadConfig,
    ThetaOptionsDownloader,
    nyse_today,
    validate_symbol,
)
from options_data.gdrive_sync import DEFAULT_GDRIVE_FOLDER_ID
from options_data.universe import DEFAULT_UNIVERSE_CSV, load_universe_symbols

console = Console()
logger = logging.getLogger(__name__)


def find_latest_downloaded_date(data_dir: Path, symbol: str) -> datetime.date | None:
    """Find the most recent downloaded trade_date for a symbol by scanning raw and processed Parquet files."""
    sym = symbol.upper()
    latest_date: datetime.date | None = None

    # 1. Search raw daily Parquet files (search backwards from the most recent year folder)
    sym_raw_dir = data_dir / "raw" / "options_eod" / f"symbol={sym}"
    if sym_raw_dir.exists():
        year_dirs = sorted(sym_raw_dir.glob("year=*"), reverse=True)
        for yd in year_dirs:
            files = sorted(yd.glob(f"{sym}_*.parquet"), reverse=True)
            for f in files:
                stem = f.stem
                date_part = stem.replace(f"{sym}_", "")
                try:
                    d = datetime.date.fromisoformat(date_part)
                    if latest_date is None or d > latest_date:
                        latest_date = d
                    break
                except ValueError:
                    continue
            if latest_date is not None:
                break

    # 2. Check processed yearly Parquet files (works even if raw files are absent or partial)
    sym_proc_dir = data_dir / "processed" / "options_eod" / f"symbol={sym}"
    if sym_proc_dir.exists():
        proc_files = sorted(sym_proc_dir.glob(f"{sym}_*.parquet"), reverse=True)
        for pf in proc_files:
            try:
                df = pl.read_parquet(pf, columns=["trade_date"])
                if not df.is_empty():
                    max_d = df["trade_date"].max()
                    if isinstance(max_d, datetime.date):
                        if latest_date is None or max_d > latest_date:
                            latest_date = max_d
                        break
            except Exception:
                pass

    return latest_date


class IncrementalOptionsUpdater:
    """Detects gaps for each symbol and downloads only missing recent trading days."""

    def __init__(
        self,
        config: DownloadConfig,
        downloader: ThetaOptionsDownloader | None = None,
    ) -> None:
        self.config = config
        self.downloader = downloader or ThetaOptionsDownloader(config=config)

    def run_update(
        self,
        target_end_date: datetime.date | None = None,
        forced_since: datetime.date | None = None,
    ) -> int:
        cfg = self.config
        end_date = target_end_date or cfg.end_date or nyse_today()

        console.rule("[bold cyan]ThetaData Options Incremental Catch-up / Daily Updater[/bold cyan]")
        console.print(
            f"  Target End Date : [bold]{end_date}[/bold]\n"
            f"  Symbols Count   : {len(cfg.symbols)}\n"
            f"  Greeks Model    : {cfg.greeks_model}\n"
            f"  Google Drive    : {'Enabled' if self.downloader.gdrive else 'Disabled'}"
        )

        # 1. Determine each symbol's starting date and find the global earliest start
        symbol_info: dict[str, tuple[datetime.date | None, str | None]] = {}
        valid_starts: list[datetime.date] = []

        for raw_sym in cfg.symbols:
            sym = raw_sym.strip().upper()
            if forced_since:
                symbol_info[sym] = (forced_since, None)
                if forced_since <= end_date:
                    valid_starts.append(forced_since)
            else:
                latest_d = find_latest_downloaded_date(cfg.data_dir, sym)
                if latest_d is None:
                    symbol_info[sym] = (None, "No existing data")
                else:
                    st = latest_d + datetime.timedelta(days=1)
                    sym_raw_dir = cfg.data_dir / "raw" / "options_eod" / f"symbol={sym}"
                    sym_proc_dir = cfg.data_dir / "processed" / "options_eod" / f"symbol={sym}"
                    covered_dates: set[datetime.date] = set()

                    if sym_raw_dir.exists():
                        for f in sym_raw_dir.glob("year=*/*"):
                            if f.name == ".empty":
                                try:
                                    yr = int(f.parent.name.replace("year=", ""))
                                    cal_days = self.downloader.get_trading_days(
                                        datetime.date(yr, 1, 1),
                                        datetime.date(yr, 12, 31),
                                    )
                                    covered_dates.update(cal_days)
                                except Exception:
                                    pass
                                continue
                            if f.suffix not in (".parquet", ".empty"):
                                continue
                            try:
                                covered_dates.add(
                                    datetime.date.fromisoformat(f.stem.replace(f"{sym}_", ""))
                                )
                            except ValueError:
                                continue

                    if sym_proc_dir.exists():
                        for pf in sym_proc_dir.glob("*.parquet"):
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

                    if covered_dates:
                        min_date = min(covered_dates)
                        try:
                            span_trading_days = self.downloader.get_trading_days(min_date, latest_d)
                            missing_holes = [d for d in span_trading_days if d not in covered_dates]
                            if missing_holes:
                                st = min(missing_holes)
                        except Exception as exc:
                            logger.debug("Could not verify internal gaps for %s: %s", sym, exc)

                    if latest_d >= end_date and st > end_date:
                        symbol_info[sym] = (None, str(latest_d))
                    else:
                        symbol_info[sym] = (st, None)
                        if st <= end_date:
                            valid_starts.append(st)

        # 2. Fetch trading calendar starting from the earliest missing gap among all symbols
        if not valid_starts:
            console.print("[green]All specified symbols are already up-to-date![/green]")
            all_trading_days = set()
        else:
            global_earliest_start = min(valid_starts)
            all_trading_days = set(self.downloader.get_trading_days(global_earliest_start, end_date))

        summary_rows = []
        failed_days = 0

        def update_symbol(raw_sym: str) -> tuple[tuple[str, str, int, str], int]:
            sym = raw_sym.strip().upper()
            start_from, reason = symbol_info.get(sym, (None, None))

            if start_from is None:
                if reason == "No existing data":
                    return ((sym, "No existing data", 0, "Skipped (use full download)"), 0)
                else:
                    return ((sym, reason or "Current", 0, "Up-to-date"), 0)

            # Missing trading days in [start_from, end_date]
            missing_days = sorted(
                [d for d in all_trading_days if start_from <= d <= end_date]
            )

            if not missing_days:
                return ((sym, str(start_from - datetime.timedelta(days=1)), 0, "Up-to-date"), 0)

            console.print(
                f"[bold yellow]→ Catching up {sym}:[/bold yellow] "
                f"{len(missing_days)} missing day(s) from {missing_days[0]} to {missing_days[-1]}"
            )

            # Stop this symbol at the first failed session. The next run resumes from
            # the latest saved file, so downloading a later day would skip the hole forever.
            # Other symbols still run; their saved dates are independent.
            saved_count = 0
            error_count = 0
            sym_failed = 0
            affected_years: set[int] = set()
            stopped_on: datetime.date | None = None

            for d in missing_days:
                try:
                    _, _rows, status = self.downloader.download_day(sym, d)
                except Exception as exc:
                    error_count += 1
                    sym_failed += 1
                    stopped_on = d
                    later = sum(1 for day in missing_days if day > d)
                    message = (
                        f"{sym} {d} failed ({exc}). "
                        f"Stopped this symbol so the next run retries {d}"
                        + (f"; {later} later day(s) were not requested." if later else ".")
                    )
                    logger.error(message)
                    console.print(f"[red]✗ {message}[/red]")
                    break
                if status == "saved":
                    saved_count += 1
                    affected_years.add(d.year)
                elif status == "not_ready":
                    logger.info("Session %s for %s is not yet available from ThetaData; will retry on next run.", d, sym)
                    break

            # Re-consolidate days that did save. The failed session is not in those files.
            if cfg.consolidate_yearly and affected_years:
                for yr in affected_years:
                    self.downloader.consolidate_year(sym, yr)

            # Update underlying stock or index daily EOD history
            if cfg.include_underlying_eod:
                try:
                    self.downloader.config.end_date = end_date
                    und_start = missing_days[0] if missing_days else cfg.start_date
                    self.downloader.download_underlying_eod(sym, start_date=und_start)
                except Exception as und_err:
                    logger.debug("Underlying EOD update skipped for %s: %s", sym, und_err)

            if error_count:
                result = f"Stopped at {stopped_on} (retry this session on the next run)"
            elif saved_count:
                result = "Updated & Synced" if self.downloader.gdrive else "Updated"
            else:
                result = "No new rows"
            return ((sym, f"{missing_days[0]} -> {missing_days[-1]}", saved_count, result), sym_failed)

        workers = min(cfg.max_workers, len(cfg.symbols)) if cfg.max_workers > 1 else 1
        if workers > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(executor.map(update_symbol, cfg.symbols))
        else:
            results = [update_symbol(sym) for sym in cfg.symbols]

        for row, sym_failed in results:
            summary_rows.append(row)
            failed_days += sym_failed

        # Print summary table
        table = Table(title="Incremental Catch-up Summary")
        table.add_column("Symbol", style="bold")
        table.add_column("Catch-up Range / Status")
        table.add_column("Days Downloaded", justify="right")
        table.add_column("Result", style="green")

        for row in summary_rows:
            table.add_row(row[0], row[1], str(row[2]), row[3])

        console.print(table)
        if failed_days:
            console.print(
                f"[bold red]Catch-up incomplete: {failed_days} trading day(s) failed. "
                "Re-run update-options; each symbol resumes at its first failed session.[/bold red]"
            )
        return failed_days


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="update-options",
        description="Incrementally catch up or daily update options data from last downloaded date to latest trading day.",
    )
    parser.add_argument(
        "-s",
        "--symbols",
        nargs="+",
        default=None,
        help="Specific symbols to update (default: all active symbols from S&P 500 universe).",
    )
    parser.add_argument(
        "--universe-file",
        type=Path,
        default=DEFAULT_UNIVERSE_CSV,
        help=f"Path to S&P 500 Universe CSV (default: {DEFAULT_UNIVERSE_CSV}).",
    )
    parser.add_argument(
        "--since",
        type=datetime.date.fromisoformat,
        default=None,
        help="Optional: Force catch up starting from this date (YYYY-MM-DD), ignoring auto-detection.",
    )
    parser.add_argument(
        "--end-date",
        type=datetime.date.fromisoformat,
        default=None,
        help="Target end date in YYYY-MM-DD (default: today's date in America/New_York).",
    )
    parser.add_argument(
        "--greeks-model",
        choices=["binomial", "bs", "none"],
        default="binomial",
        help="Greeks model: 'binomial' (Cox-Ross-Rubinstein American style, recommended default for equity options/CSP/CC), 'bs' (Black-Scholes European style), or 'none'. Default: binomial",
    )
    parser.add_argument(
        "--annual-dividend",
        type=float,
        default=None,
        help="Annualized dividend yield as a decimal (e.g. 0.015 for 1.5%%) for Greeks calculation.",
    )
    parser.add_argument(
        "--annual-dividend-amount",
        type=float,
        default=None,
        help="Annual dividend in dollars (e.g. 1.50) for Greeks calculation.",
    )
    parser.add_argument(
        "--strike-range",
        type=int,
        default=50,
        help="Optional number of strikes above/below ATM to fetch (default: 50).",
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        default=150,
        help="Optional max Days-to-Expiration filter (default: 150).",
    )
    parser.add_argument(
        "--no-open-interest",
        dest="include_open_interest",
        action="store_false",
        default=True,
        help="Skip open interest.",
    )
    parser.add_argument(
        "--skip-underlying",
        action="store_true",
        default=False,
        help="Skip downloading/updating underlying stock/index daily EOD data.",
    )
    parser.add_argument(
        "--no-gdrive",
        action="store_true",
        help="Skip uploading updated yearly Parquet to Google Drive.",
    )
    parser.add_argument(
        "--gdrive-folder-id",
        default=os.getenv("GDRIVE_FOLDER_ID", DEFAULT_GDRIVE_FOLDER_ID),
        help=f"Google Drive folder ID (default: {DEFAULT_GDRIVE_FOLDER_ID}).",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=int(os.getenv("THETADATA_MAX_WORKERS", "4")),
        help="Concurrent request threads (default: 4).",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Data directory path (default: ./data).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.end_date is None:
        args.end_date = nyse_today()

    if args.symbols:
        symbols = [validate_symbol(s) for s in args.symbols if s.strip()]
    else:
        symbols = [validate_symbol(s) for s in load_universe_symbols(csv_path=args.universe_file, mode="active")]

    config = DownloadConfig(
        symbols=symbols,
        start_date=args.since or (args.end_date - datetime.timedelta(days=90)),
        end_date=args.end_date,
        data_dir=args.data_dir,
        greeks_model=args.greeks_model,
        annual_dividend=args.annual_dividend,
        annual_dividend_amount=args.annual_dividend_amount,
        max_workers=args.workers,
        sync_to_gdrive=not args.no_gdrive,
        gdrive_folder_id=args.gdrive_folder_id,
        skip_existing=True,
        consolidate_yearly=True,
        strike_range=args.strike_range,
        max_dte=args.max_dte,
        include_open_interest=args.include_open_interest,
        include_underlying_eod=not args.skip_underlying,
    )

    try:
        updater = IncrementalOptionsUpdater(config=config)
    except AuthenticationError as exc:
        console.print(f"[bold red]Authentication Error:[/bold red] {exc}")
        return 1

    failed_days = updater.run_update(target_end_date=args.end_date, forced_since=args.since)
    return 1 if failed_days else 0


if __name__ == "__main__":
    sys.exit(main())
