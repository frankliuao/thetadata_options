"""Command-line interface for downloading historical Options EOD + Greeks from ThetaData."""

from __future__ import annotations

import argparse
import datetime
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
    load_merged_options_year,
    nyse_today,
    validate_symbol,
)
from options_data.gdrive_sync import DEFAULT_GDRIVE_FOLDER_ID, GoogleDriveSync
from options_data.loader import OptionsDataLoader
from options_data.schema import normalize_options_df, write_parquet_atomic
from options_data.universe import (
    DEFAULT_UNIVERSE_CSV,
    DEFAULT_UNIVERSE_SHEET_ID,
    export_universe_sheet_from_gdrive,
    load_universe_symbols,
)

console = Console()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="download-options",
        description="Download 2016–present daily Options EOD + Greeks + Open Interest into Parquet and Google Drive.",
    )
    parser.add_argument(
        "-s",
        "--symbols",
        nargs="+",
        default=None,
        help="Explicit ticker symbols to download (e.g. --symbols SPY QQQ AAPL). Overrides --universe if specified.",
    )
    parser.add_argument(
        "-u",
        "--universe",
        choices=["active", "current", "removed_active", "all"],
        default="active",
        help=(
            "S&P 500 Universe filter when --symbols is not provided: "
            "'active' (575 stocks: 503 Current + 72 Removed but Active, default), "
            "'current' (503 Current S&P 500 only), "
            "'removed_active' (72 Removed but Active only), "
            "'all' (575 Active + 51 Excluded/Delisted)."
        ),
    )
    parser.add_argument(
        "--universe-file",
        type=Path,
        default=DEFAULT_UNIVERSE_CSV,
        help=f"Path to S&P 500 Universe CSV file (default: {DEFAULT_UNIVERSE_CSV}).",
    )
    parser.add_argument(
        "--refresh-universe",
        action="store_true",
        help="Re-export the S&P 500 Universe Google Sheet from Google Drive to config/sp500_universe.csv.",
    )
    parser.add_argument(
        "--include-historical-tickers",
        action="store_true",
        help="Also include historical pre-rename tickers (e.g. ADS, HFC, FBHS, GPS, WLTW) from the universe.",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Start index offset into the symbol list (default: 0).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of symbols to process from the symbol list (e.g. --limit 10).",
    )
    parser.add_argument(
        "--list-universe",
        action="store_true",
        help="Print the resolved symbol list from the S&P 500 universe and exit.",
    )
    parser.add_argument(
        "--start-date",
        type=datetime.date.fromisoformat,
        default=datetime.date(2016, 1, 1),
        help="Start date in YYYY-MM-DD format (default: 2016-01-01).",
    )
    parser.add_argument(
        "--end-date",
        type=datetime.date.fromisoformat,
        default=None,
        help="End date in YYYY-MM-DD format (default: today's date in America/New_York).",
    )
    parser.add_argument(
        "--greeks-model",
        choices=["binomial", "bs", "none"],
        default="binomial",
        help="Greeks model: 'binomial' (Cox-Ross-Rubinstein American style, recommended default for equity options/CSP/CC), 'bs' (Black-Scholes European style), or 'none' (EOD OHLC+NBBO only). Default: binomial",
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
        "--rate-type",
        default="sofr",
        help="Interest rate benchmark for Greeks calculation (default: sofr).",
    )
    parser.add_argument(
        "--max-dte",
        type=int,
        default=None,
        help="Optional max Days-to-Expiration filter (e.g. 365) to reduce data size.",
    )
    parser.add_argument(
        "--strike-range",
        type=int,
        default=None,
        help="Optional number of strikes above/below ATM to fetch (default: all strikes).",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=int(os.getenv("THETADATA_MAX_WORKERS", "4")),
        help="Concurrent request threads (Standard tier=4, Pro tier=8, Value tier=2). Default: 4",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Output directory for Parquet files (default: ./data).",
    )
    parser.add_argument(
        "--no-open-interest",
        action="store_true",
        help="Skip fetching and joining daily Open Interest.",
    )
    parser.add_argument(
        "--no-underlying",
        action="store_true",
        help="Skip downloading underlying stock/index EOD history.",
    )
    parser.add_argument(
        "--no-consolidate",
        action="store_true",
        help="Skip consolidating daily Parquet files into yearly Parquet files.",
    )
    parser.add_argument(
        "--gdrive-folder-id",
        default=os.getenv("GDRIVE_FOLDER_ID", DEFAULT_GDRIVE_FOLDER_ID),
        help=f"Google Drive target folder ID (default: {DEFAULT_GDRIVE_FOLDER_ID} 'Options Strategy/thetadata_parquet').",
    )
    parser.add_argument(
        "--no-gdrive",
        action="store_true",
        help="Disable automatic upload to Google Drive.",
    )
    parser.add_argument(
        "--gdrive-upload-daily",
        action="store_true",
        help="Also upload individual daily raw Parquet files to Google Drive (in addition to consolidated yearly Parquet files).",
    )
    parser.add_argument(
        "--sync-only",
        action="store_true",
        help="Consolidate and upload already-downloaded local Parquet files in --data-dir to Google Drive without querying ThetaData.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing daily Parquet files instead of resuming.",
    )
    parser.add_argument(
        "--sheet-id",
        type=str,
        default=None,
        help="Google Sheet ID for S&P 500 universe export (overrides SP500_UNIVERSE_SHEET_ID env var).",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="ThetaData API key (overrides THETADATA_API_KEY env var / .env file).",
    )
    parser.add_argument(
        "--verify-auth",
        action="store_true",
        help="Only verify ThetaData authentication and print subscription levels, then exit.",
    )
    parser.add_argument(
        "--mock-test",
        action="store_true",
        help="Run a local self-test with synthetic ThetaData EOD + Greeks data (no API key needed).",
    )
    return parser


def resolve_symbols(args: argparse.Namespace) -> list[str]:
    """Resolve the list of symbols from CLI flags or the S&P 500 Universe CSV."""
    sheet_id = getattr(args, "sheet_id", None)
    if args.refresh_universe:
        export_universe_sheet_from_gdrive(
            sheet_id=sheet_id,
            clean_csv_path=args.universe_file,
        )

    if args.symbols:
        syms = [s.strip().upper() for s in args.symbols if s.strip()]
    else:
        syms = load_universe_symbols(
            csv_path=args.universe_file,
            mode=args.universe,
            include_historical_tickers=args.include_historical_tickers,
            sheet_id=sheet_id,
        )

    if args.offset > 0:
        syms = syms[args.offset :]
    if args.limit is not None:
        syms = syms[: args.limit]
    return [validate_symbol(sym) for sym in syms]


def run_sync_only(data_dir: Path, symbols: list[str], gdrive_folder_id: str) -> int:
    """Consolidate and upload existing local Parquet files to Google Drive."""
    console.rule("[bold blue]Syncing Local Parquet Data to Google Drive[/bold blue]")
    sync = GoogleDriveSync(root_folder_id=gdrive_folder_id)

    for raw_sym in symbols:
        sym = validate_symbol(raw_sym)
        underlying_file = data_dir / "raw" / "underlying_eod" / f"{sym}_eod.parquet"
        if underlying_file.exists():
            sync.upload_underlying_parquet(underlying_file)

        raw_sym_dir = data_dir / "raw" / "options_eod" / f"symbol={sym}"
        if not raw_sym_dir.exists():
            continue
        for year_dir in sorted(raw_sym_dir.glob("year=*")):
            if not year_dir.is_dir():
                continue
            year_str = year_dir.name.split("=", 1)[-1]
            if not year_str.isdigit() or not any(year_dir.glob("*.parquet")):
                continue
            year = int(year_str)
            df = load_merged_options_year(data_dir, sym, year)
            if df is None:
                continue
            out_path = (
                data_dir / "processed" / "options_eod" / f"symbol={sym}" / f"{sym}_{year}.parquet"
            )
            write_parquet_atomic(df, out_path)
            sync.upload_yearly_options_parquet(sym, out_path)
    return 0


def run_mock_test(data_dir: Path) -> int:
    """Verify the entire schema normalization, Parquet storage, and Polars LazyFrame loader offline."""
    console.rule("[bold green]Running Offline Mock Pipeline Verification[/bold green]")
    mock_dir = data_dir / "mock_verify"
    raw_df = pl.DataFrame(
        {
            "symbol": ["SPY", "SPY", "SPY", "SPY"],
            "expiration": ["2024-12-20", "2024-12-20", "2025-01-17", "2025-01-17"],
            "strike": [580.0, 580.0, 600.0, 600.0],
            "right": ["call", "put", "call", "put"],
            "open": [12.5, 8.1, 10.2, 14.3],
            "high": [13.2, 8.6, 11.0, 15.0],
            "low": [12.1, 7.8, 9.9, 13.8],
            "close": [12.9, 8.0, 10.8, 14.1],
            "volume": [15420, 12310, 8420, 6150],
            "count": [1240, 980, 610, 490],
            "bid_size": [150, 200, 120, 180],
            "bid_exchange": [1, 1, 1, 1],
            "bid": [12.85, 7.95, 10.75, 14.05],
            "bid_condition": [0, 0, 0, 0],
            "ask_size": [140, 190, 110, 160],
            "ask_exchange": [1, 1, 1, 1],
            "ask": [12.95, 8.05, 10.85, 14.15],
            "ask_condition": [0, 0, 0, 0],
            "delta": [0.542, -0.458, 0.412, -0.588],
            "theta": ["-0.215", "-0.198", "-0.165", "-0.152"],
            "vega": [0.624, 0.624, 0.891, 0.891],
            "rho": [0.185, -0.162, 0.294, -0.341],
            "gamma": [0.0182, 0.0182, 0.0124, 0.0124],
            "vanna": ["0.041", "-0.039", "0.052", "-0.048"],
            "charm": [-0.003, 0.003, -0.002, 0.002],
            "vomma": [0.012, 0.012, 0.019, 0.019],
            "implied_vol": [0.148, 0.149, 0.156, 0.158],
            "iv_error": [0.0001, 0.0001, 0.0002, 0.0002],
            "underlying_price": [584.50, 584.50, 584.50, 584.50],
            "open_interest": [48200, 53100, 31900, 29400],
        }
    )

    trade_date = datetime.date(2024, 11, 15)
    norm_df = normalize_options_df(raw_df, trade_date=trade_date)

    daily_path = (
        mock_dir
        / "raw"
        / "options_eod"
        / "symbol=SPY"
        / "year=2024"
        / "SPY_2024-11-15.parquet"
    )
    write_parquet_atomic(norm_df, daily_path)

    loader = OptionsDataLoader(data_dir=mock_dir)
    queried = loader.query_chain(
        "SPY",
        start_date="2024-11-01",
        end_date="2024-12-01",
        right="call",
        min_dte=20,
        max_dte=90,
        min_abs_delta=0.30,
    )

    table = Table(title="Mock Parquet Round-Trip Verification (SPY Calls, DTE 20..90)")
    for col in [
        "trade_date",
        "symbol",
        "expiration",
        "dte",
        "strike",
        "right",
        "bid",
        "ask",
        "mid",
        "open_interest",
        "implied_vol",
        "delta",
        "gamma",
        "theta",
        "vega",
        "underlying_price",
    ]:
        table.add_column(col)

    for row in queried.select(
        [
            "trade_date",
            "symbol",
            "expiration",
            "dte",
            "strike",
            "right",
            "bid",
            "ask",
            "mid",
            "open_interest",
            "implied_vol",
            "delta",
            "gamma",
            "theta",
            "vega",
            "underlying_price",
        ]
    ).iter_rows():
        table.add_row(*[str(x) for x in row])

    console.print(table)
    calls = queried.filter(pl.col("right") == "call")
    dtes = set(calls["dte"].to_list()) if calls.height else set()
    if calls.height != 2 or dtes != {35, 63}:
        console.print(
            f"[bold red]✗ Mock round-trip failed:[/bold red] expected 2 calls with DTE {{35, 63}}, "
            f"got {calls.height} rows and DTE {sorted(dtes)}."
        )
        return 1
    console.print(f"[green]✓ Parquet written and queried successfully at:[/green] {daily_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.mock_test:
        return run_mock_test(args.data_dir)

    if args.end_date is None:
        args.end_date = nyse_today()

    symbols = resolve_symbols(args)

    if args.list_universe:
        console.print(
            f"[bold cyan]Resolved Symbols ({len(symbols)} total, mode={args.universe}):[/bold cyan]\n"
            + ", ".join(symbols)
        )
        return 0

    if args.sync_only:
        return run_sync_only(args.data_dir, symbols, args.gdrive_folder_id)

    config = DownloadConfig(
        symbols=symbols,
        start_date=args.start_date,
        end_date=args.end_date,
        data_dir=args.data_dir,
        greeks_model=args.greeks_model,
        annual_dividend=args.annual_dividend,
        annual_dividend_amount=args.annual_dividend_amount,
        include_open_interest=not args.no_open_interest,
        include_underlying_eod=not args.no_underlying,
        rate_type=args.rate_type,
        max_dte=args.max_dte,
        strike_range=args.strike_range,
        max_workers=args.workers,
        skip_existing=not args.overwrite,
        consolidate_yearly=not args.no_consolidate,
        sync_to_gdrive=not args.no_gdrive,
        gdrive_folder_id=args.gdrive_folder_id,
        gdrive_upload_daily=args.gdrive_upload_daily,
        api_key=args.api_key,
    )

    try:
        downloader = ThetaOptionsDownloader(config=config)
    except AuthenticationError as exc:
        console.print(
            "[bold red]Authentication Error:[/bold red] Could not authenticate with ThetaData.\n"
            "  1. Copy [cyan].env.example[/cyan] to [cyan].env[/cyan]\n"
            "  2. Set [cyan]THETADATA_API_KEY=\"your_real_api_key\"[/cyan] in [cyan].env[/cyan]\n"
            f"  Details: {exc}"
        )
        return 1

    if args.verify_auth:
        c = downloader.main_client
        console.print(
            f"[bold green]✓ Successfully authenticated with ThetaData![/bold green]\n"
            f"  Email                : {c.email}\n"
            f"  Options Subscription : [bold]{c.options_subscription}[/bold]\n"
            f"  Stock Subscription   : [bold]{c.stock_subscription}[/bold]\n"
            f"  Index Subscription   : [bold]{c.index_subscription}[/bold]"
        )
        return 0

    downloader.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
