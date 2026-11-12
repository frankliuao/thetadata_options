"""Historical Options EOD + Greeks downloader powered by ThetaData Python Library (v3)."""

from __future__ import annotations

import concurrent.futures
import datetime
import logging
import os
import re
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

# Ensure gRPC keepalive is active to prevent hung connections during long network idle/drops
os.environ.setdefault("GRPC_ARG_KEEPALIVE_TIME_MS", "30000")
os.environ.setdefault("GRPC_ARG_KEEPALIVE_TIMEOUT_MS", "10000")
os.environ.setdefault("GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA", "0")
os.environ.setdefault("GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS", "1")

import grpc
import polars as pl
from dotenv import load_dotenv
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)
from thetadata import ThetaClient
from thetadata.errors import AuthenticationError, NoDataFoundError

from options_data.gdrive_sync import DEFAULT_GDRIVE_FOLDER_ID, GoogleDriveSync
from options_data.schema import (
    _SYMBOL_RE,
    date_expr,
    merge_option_frames,
    merge_underlying_frames,
    normalize_options_df,
    validate_symbol,
    write_parquet_atomic,
)

logger = logging.getLogger(__name__)
console = Console()

GreeksModel = Literal["bs", "binomial", "none"]

INDEX_ROOTS: frozenset[str] = frozenset(
    {"SPX", "SPXW", "XSP", "NDX", "NDXP", "XND", "RUT", "RUTW", "VIX", "VIXW", "DJX", "XEO"}
)

def nyse_today() -> datetime.date:
    """Session calendar date. ThetaData EOD is stamped in America/New_York, not UTC."""
    return datetime.datetime.now(ZoneInfo("America/New_York")).date()


def to_option_root(symbol: str) -> str:
    """Convert an equity ticker to ThetaData option root symbology.

    ThetaData uses OCC option roots without dots (e.g. BRKB, BFB).
    Stock endpoints use standard market tickers with dots (e.g. BRK.B, BF.B).
    """
    return symbol.strip().upper().replace(".", "")


def load_merged_options_year(data_dir: Path, symbol: str, year: int) -> pl.DataFrame | None:
    """Merge the processed yearly file with that year's raw daily files.

    The processed file is included so a partial raw directory cannot drop days
    that were already consolidated. Unreadable files are skipped and logged.
    """
    sym = validate_symbol(symbol)
    frames: list[pl.DataFrame] = []
    processed = (
        data_dir / "processed" / "options_eod" / f"symbol={sym}" / f"{sym}_{year}.parquet"
    )
    if processed.exists() and processed.stat().st_size > 0:
        try:
            frames.append(pl.read_parquet(processed))
        except Exception as exc:
            logger.warning("Failed to read existing yearly parquet %s: %s", processed, exc)

    year_dir = data_dir / "raw" / "options_eod" / f"symbol={sym}" / f"year={year}"
    if year_dir.exists():
        for path in sorted(year_dir.glob("*.parquet")):
            try:
                frames.append(pl.read_parquet(path))
            except Exception as exc:
                logger.warning("Failed to read raw parquet %s: %s", path, exc)

    if not frames:
        return None
    merged = merge_option_frames(frames)
    if merged.is_empty():
        return None
    return merged


def _is_transient_rpc_error(exc: BaseException) -> bool:
    """Return True if the exception is a transient gRPC/network error worth retrying."""
    if isinstance(exc, (NoDataFoundError, AuthenticationError)):
        return False
    if isinstance(exc, grpc.RpcError):
        code = exc.code()
        if code in {
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            grpc.StatusCode.INTERNAL,
            grpc.StatusCode.UNKNOWN,
            grpc.StatusCode.UNAUTHENTICATED,  # Allow retry after refreshing session token
        }:
            return True
        details = str(exc.details() if hasattr(exc, "details") else exc)
        if "Wrong number of data fields" in details or "Corrupt" in details or "stream" in details.lower():
            return True
        return False
    return isinstance(exc, (TimeoutError, ConnectionError))


@dataclass(slots=True)
class DownloadConfig:
    """Configuration for downloading historical Options EOD + Greeks data."""

    symbols: list[str]
    start_date: datetime.date = field(default_factory=lambda: datetime.date(2016, 1, 1))
    end_date: datetime.date = field(default_factory=nyse_today)
    data_dir: Path = field(default_factory=lambda: Path("data"))
    greeks_model: GreeksModel = "binomial"
    include_open_interest: bool = True
    include_underlying_eod: bool = True
    fallback_eod_if_no_greeks: bool = True
    rate_type: str = "sofr"
    annual_dividend: float | None = None
    annual_dividend_amount: float | None = None
    option_use_nbbo: bool = True
    underlyer_use_nbbo: bool = False
    max_dte: int | None = None
    strike_range: int | None = None
    binomial_steps: int = 101
    max_workers: int = 4
    skip_existing: bool = True
    consolidate_yearly: bool = True
    sync_to_gdrive: bool = True
    gdrive_folder_id: str = field(
        default_factory=lambda: os.getenv("GDRIVE_FOLDER_ID", DEFAULT_GDRIVE_FOLDER_ID)
    )
    gdrive_upload_daily: bool = False
    api_key: str | None = None
    dotenv_path: Path | None = None
    rpc_timeout: float = 60.0


class ThetaOptionsDownloader:
    """Downloads 2016–present daily Options EOD + Greeks + Open Interest into Parquet & Google Drive."""

    def __init__(self, config: DownloadConfig, client: ThetaClient | None = None) -> None:
        self.config = config
        load_dotenv(dotenv_path=config.dotenv_path)
        self._thread_local = threading.local()
        self._auth_lock = threading.Lock()
        self._last_auth_time = time.monotonic()
        self.gdrive = (
            GoogleDriveSync(root_folder_id=config.gdrive_folder_id)
            if config.sync_to_gdrive and config.gdrive_folder_id
            else None
        )
        self.main_client = client or self._create_client()

    def _apply_rpc_timeout(self, client: ThetaClient) -> None:
        """Wrap ThetaClient gRPC stubs to enforce rpc_timeout on otherwise infinite calls."""
        timeout = getattr(self.config, "rpc_timeout", 60.0)
        if not timeout or timeout <= 0:
            return
        stub = getattr(client, "stub", None)
        if stub is None:
            return
        for attr in dir(stub):
            if attr.startswith("Get") or attr.startswith("List"):
                method = getattr(stub, attr)
                if callable(method) and not getattr(method, "_has_rpc_timeout", False):
                    def make_wrapper(orig_method):
                        def wrapper(request, *args, **kwargs):
                            kwargs.setdefault("timeout", timeout)
                            return orig_method(request, *args, **kwargs)
                        wrapper._has_rpc_timeout = True  # type: ignore[attr-defined]
                        return wrapper
                    setattr(stub, attr, make_wrapper(method))

    def _create_client(self) -> ThetaClient:
        """Create a new authorized ThetaClient instance."""
        api_key = self.config.api_key or os.getenv("THETADATA_API_KEY")
        if api_key and api_key != "your_api_key_here":
            client = ThetaClient(
                api_key=api_key,
                dataframe_type="polars",
                dotenv_path=self.config.dotenv_path,
            )
        else:
            client = ThetaClient(
                dataframe_type="polars",
                dotenv_path=self.config.dotenv_path,
            )
        self._apply_rpc_timeout(client)
        return client

    def _reauthenticate(self, force: bool = False) -> None:
        """Thread-safe session re-authentication when session expires (ThetaData ~15min TTL)."""
        with self._auth_lock:
            if not force and (time.monotonic() - self._last_auth_time < 5.0):
                return
            try:
                self.main_client = self._create_client()
                self._last_auth_time = time.monotonic()
                console.print("[yellow]⚡ ThetaData session refreshed.[/yellow]")
            except Exception as exc:
                logger.error("Failed to re-authenticate with ThetaData: %s", exc)

    def _get_thread_client(self) -> ThetaClient:
        """Return a thread-local ThetaClient reusing the main client's auth token."""
        # Proactively refresh session every 8 minutes (480s) to prevent unexpected expiration
        if time.monotonic() - self._last_auth_time > 480.0:
            self._reauthenticate()

        curr_session = getattr(self.main_client.auth_token, "session_uuid", None)
        client = getattr(self._thread_local, "client", None)
        cached_session = getattr(self._thread_local, "session_uuid", None)

        if client is None or cached_session != curr_session:
            client = ThetaClient(
                existing_authorized_client=self.main_client,
                dataframe_type="polars",
            )
            self._apply_rpc_timeout(client)
            self._thread_local.client = client
            self._thread_local.session_uuid = curr_session
        return client

    def get_trading_days(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> list[datetime.date]:
        """Fetch official market trading days in [start_date, end_date] via ThetaData calendar."""
        trading_days: set[datetime.date] = set()
        client = self.main_client

        for year in range(start_date.year, end_date.year + 1):
            full_close_dates: set[datetime.date] = set()
            try:
                cal_df = client.calendar_year(str(year))
                if not cal_df.is_empty() and "date" in cal_df.columns:
                    cal_df = cal_df.with_columns(
                        date_expr("date", cal_df.schema["date"]).alias("date")
                    )

                    if "type" in cal_df.columns:
                        closed = cal_df.filter(
                            pl.col("type").cast(pl.Utf8).str.to_lowercase() == "full_close"
                        )
                        for d in closed["date"].to_list():
                            if isinstance(d, datetime.date):
                                full_close_dates.add(d)
            except Exception as exc:
                logger.warning(
                    "calendar_year(%s) failed (%s); using default NYSE weekday schedule.",
                    year,
                    exc,
                )

            cur = max(start_date, datetime.date(year, 1, 1))
            yr_end = min(end_date, datetime.date(year, 12, 31))
            while cur <= yr_end:
                if cur.weekday() < 5 and cur not in full_close_dates:
                    trading_days.add(cur)
                cur += datetime.timedelta(days=1)

        return sorted(trading_days)

    def daily_parquet_path(self, symbol: str, trade_date: datetime.date) -> Path:
        """Return the partitioned path for a single symbol and trading day."""
        sym = validate_symbol(symbol)
        return (
            self.config.data_dir
            / "raw"
            / "options_eod"
            / f"symbol={sym}"
            / f"year={trade_date.year}"
            / f"{sym}_{trade_date.isoformat()}.parquet"
        )

    def yearly_parquet_path(self, symbol: str, year: int) -> Path:
        """Return the consolidated yearly Parquet path for a symbol and year."""
        sym = validate_symbol(symbol)
        return (
            self.config.data_dir
            / "processed"
            / "options_eod"
            / f"symbol={sym}"
            / f"{sym}_{year}.parquet"
        )

    @retry(
        retry=retry_if_exception(_is_transient_rpc_error),
        wait=wait_exponential(multiplier=1.0, min=1.0, max=15.0),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _fetch_single_day_options(
        self,
        symbol: str,
        trade_date: datetime.date,
    ) -> pl.DataFrame:
        """Fetch EOD + Greeks (+ Open Interest) for all expirations on `trade_date`."""
        client = self._get_thread_client()
        cfg = self.config
        opt_root = to_option_root(symbol)
        df = pl.DataFrame()

        div_kwargs: dict[str, float] = {}
        if cfg.annual_dividend is not None:
            div_kwargs["annual_dividend"] = cfg.annual_dividend
        elif cfg.annual_dividend_amount is not None:
            div_kwargs["annual_dividend_amount"] = cfg.annual_dividend_amount

        try:
            if cfg.greeks_model == "bs":
                df = client.option_history_greeks_eod(
                    symbol=opt_root,
                    expiration="*",
                    start_date=trade_date,
                    end_date=trade_date,
                    strike="*",
                    right="both",
                    rate_type=cfg.rate_type,
                    underlyer_use_nbbo=cfg.underlyer_use_nbbo,
                    option_use_nbbo=cfg.option_use_nbbo,
                    max_dte=cfg.max_dte,
                    strike_range=cfg.strike_range,
                    **div_kwargs,
                )
            elif cfg.greeks_model == "binomial":
                df = client.option_history_binomial_greeks_eod(
                    symbol=opt_root,
                    expiration="*",
                    start_date=trade_date,
                    end_date=trade_date,
                    strike="*",
                    right="both",
                    rate_type=cfg.rate_type,
                    binomial_steps=cfg.binomial_steps,
                    underlyer_use_nbbo=cfg.underlyer_use_nbbo,
                    option_use_nbbo=cfg.option_use_nbbo,
                    max_dte=cfg.max_dte,
                    strike_range=cfg.strike_range,
                    **div_kwargs,
                )
            else:
                df = client.option_history_eod(
                    symbol=opt_root,
                    expiration="*",
                    start_date=trade_date,
                    end_date=trade_date,
                    strike="*",
                    right="both",
                    max_dte=cfg.max_dte,
                    strike_range=cfg.strike_range,
                )
        except grpc.RpcError as rpc_err:
            if rpc_err.code() == grpc.StatusCode.UNAUTHENTICATED:
                self._reauthenticate(force=True)
                raise rpc_err
            if rpc_err.code() == grpc.StatusCode.INVALID_ARGUMENT and "current-day data" in str(rpc_err.details()):
                raise RuntimeError(
                    f"ThetaData current-day EOD not ready for {trade_date} (usually available after 17:15 ET): {rpc_err.details()}"
                )
            raise rpc_err
        except NoDataFoundError:
            if cfg.greeks_model != "none" and cfg.fallback_eod_if_no_greeks:
                try:
                    df = client.option_history_eod(
                        symbol=opt_root,
                        expiration="*",
                        start_date=trade_date,
                        end_date=trade_date,
                        strike="*",
                        right="both",
                        max_dte=cfg.max_dte,
                        strike_range=cfg.strike_range,
                    )
                except grpc.RpcError as rpc_err:
                    if rpc_err.code() == grpc.StatusCode.UNAUTHENTICATED:
                        self._reauthenticate(force=True)
                        raise rpc_err
                    if rpc_err.code() == grpc.StatusCode.INVALID_ARGUMENT and "current-day data" in str(rpc_err.details()):
                        raise RuntimeError(
                            f"ThetaData current-day EOD not ready for {trade_date} (usually available after 17:15 ET): {rpc_err.details()}"
                        )
                    raise rpc_err
                except NoDataFoundError:
                    return pl.DataFrame()
            else:
                return pl.DataFrame()

        if df.is_empty():
            return df

        if "symbol" in df.columns:
            df = df.with_columns(pl.lit(symbol).alias("symbol"))
        df = normalize_options_df(df, trade_date=trade_date)

        if cfg.include_open_interest:
            # The value published on `trade_date` is the prior session's open interest
            # (OPRA reports it around 06:30 ET). It is the figure known during this
            # session, not the open interest at this session's close.
            try:
                oi_df = client.option_history_open_interest(
                    symbol=opt_root,
                    expiration="*",
                    date=trade_date,
                    strike="*",
                    right="both",
                    max_dte=cfg.max_dte,
                    strike_range=cfg.strike_range,
                )
                if not oi_df.is_empty():
                    if "symbol" in oi_df.columns:
                        oi_df = oi_df.with_columns(pl.lit(symbol).alias("symbol"))
                    oi_df = normalize_options_df(oi_df, trade_date=trade_date)
                    join_keys = ["symbol", "expiration", "strike", "right"]
                    if all(k in oi_df.columns for k in join_keys) and "open_interest" in oi_df.columns:
                        oi_subset = (
                            oi_df.select(join_keys + ["open_interest"])
                            .unique(subset=join_keys, keep="last")
                        )
                        for col in ("open_interest", "open_interest_prior_close"):
                            if col in df.columns:
                                df = df.drop(col)
                        df = df.join(oi_subset, on=join_keys, how="left")
                        df = normalize_options_df(df, trade_date=trade_date)
            except NoDataFoundError:
                pass
            except grpc.RpcError as rpc_err:
                if rpc_err.code() == grpc.StatusCode.UNAUTHENTICATED:
                    self._reauthenticate(force=True)
                # Re-raise so transient errors retry and fatal errors abort the day,
                # preventing corrupted or incomplete open-interest data from being permanently saved.
                raise

        return df

    def download_day(self, symbol: str, trade_date: datetime.date) -> tuple[datetime.date, int, str]:
        """Download a single trading day for `symbol` and save to Parquet."""
        out_path = self.daily_parquet_path(symbol, trade_date)
        empty_marker = out_path.with_suffix(".empty")
        if self.config.skip_existing:
            if (out_path.exists() and out_path.stat().st_size > 0) or empty_marker.exists():
                return (trade_date, 0, "skipped")
            proc_path = self.yearly_parquet_path(symbol, trade_date.year)
            if proc_path.exists() and proc_path.stat().st_size > 0:
                try:
                    has_date = (
                        pl.scan_parquet(str(proc_path))
                        .filter(pl.col("trade_date") == trade_date)
                        .select(pl.len())
                        .collect()
                        .item()
                        > 0
                    )
                    if has_date:
                        return (trade_date, 0, "skipped")
                except Exception as read_err:
                    logger.debug("Could not verify %s in %s: %s", trade_date, proc_path, read_err)

        df = self._fetch_single_day_options(symbol, trade_date)
        if df.is_empty():
            today_et = datetime.datetime.now(ZoneInfo("America/New_York")).date()
            if trade_date < today_et:
                # Record that this past trading day had no option contracts/data, so skip_existing
                # and hole-scanning in updater won't re-request this empty day every run.
                empty_marker.parent.mkdir(parents=True, exist_ok=True)
                empty_marker.touch()
                return (trade_date, 0, "empty")
            else:
                # Current or future session: never write permanent .empty sentinel as EOD data
                # might still be pending generation or market close.
                return (trade_date, 0, "not_ready")

        # If valid data is returned, clean up any stale empty sentinel
        if empty_marker.exists():
            empty_marker.unlink(missing_ok=True)

        write_parquet_atomic(df, out_path)
        if self.gdrive is not None and self.config.gdrive_upload_daily:
            try:
                self.gdrive.upload_daily_options_parquet(symbol, trade_date.year, out_path)
            except Exception as gdrive_exc:
                logger.warning("Daily Drive upload failed for %s %s: %s", symbol, trade_date, gdrive_exc)
        return (trade_date, df.height, "saved")

    def download_underlying_eod(self, symbol: str, start_date: datetime.date | None = None) -> Path | None:
        """Download underlying stock or index EOD history and upload to Google Drive."""
        sym = validate_symbol(symbol)
        out_path = self.config.data_dir / "raw" / "underlying_eod" / f"{sym}_eod.parquet"
        fetch_start = start_date or self.config.start_date
        if out_path.exists() and out_path.stat().st_size > 0:
            try:
                existing_scan = pl.scan_parquet(str(out_path))
                cols = existing_scan.collect_schema().names()
                date_col = "trade_date" if "trade_date" in cols else ("date" if "date" in cols else None)
                if date_col:
                    latest_existing = existing_scan.select(pl.col(date_col).max()).collect().item()
                    if latest_existing is not None:
                        if isinstance(latest_existing, str):
                            latest_existing = datetime.date.fromisoformat(latest_existing)
                        elif isinstance(latest_existing, datetime.datetime):
                            latest_existing = latest_existing.date()
                        if start_date is None and latest_existing < fetch_start:
                            fetch_start = latest_existing
            except Exception as exc:
                logger.debug("Could not inspect existing underlying EOD file for %s: %s", sym, exc)

        try:
            if sym in INDEX_ROOTS:
                df = self.main_client.index_history_eod(
                    symbol=sym,
                    start_date=fetch_start,
                    end_date=self.config.end_date,
                )
            else:
                df = self.main_client.stock_history_eod(
                    symbol=sym,
                    start_date=fetch_start,
                    end_date=self.config.end_date,
                )
            if not df.is_empty():
                if out_path.exists() and out_path.stat().st_size > 0:
                    df = merge_underlying_frames(pl.read_parquet(out_path), df)
                write_parquet_atomic(df, out_path)
                console.print(
                    f"[green]✓[/green] Saved underlying EOD for [bold]{sym}[/bold] "
                    f"({df.height:,} rows) -> {out_path}"
                )
                if self.gdrive is not None:
                    self.gdrive.upload_underlying_parquet(out_path)
                return out_path
        except NoDataFoundError:
            pass
        except Exception as exc:
            logger.debug("Could not download separate underlying EOD for %s: %s", sym, exc)
        return None

    def consolidate_year(self, symbol: str, year: int) -> Path | None:
        """Consolidate all daily Parquet files for `(symbol, year)` and upload to Google Drive.

        Safely preserves existing processed yearly data so partial raw files during catch-up
        never overwrite or drop days already saved in processed yearly Parquet.
        """
        sym = validate_symbol(symbol)
        out_path = self.yearly_parquet_path(sym, year)
        df = load_merged_options_year(self.config.data_dir, sym, year)
        if df is None:
            return None
        raw_count = 0
        year_dir = self.config.data_dir / "raw" / "options_eod" / f"symbol={sym}" / f"year={year}"
        if year_dir.exists():
            raw_count = len(list(year_dir.glob("*.parquet")))

        write_parquet_atomic(df, out_path)
        total_days = df["trade_date"].n_unique() if "trade_date" in df.columns else raw_count
        console.print(
            f"  [cyan]→[/cyan] Consolidated [bold]{sym} {year}[/bold]: "
            f"{total_days} days, {df.height:,} contracts -> {out_path}"
        )
        if self.gdrive is not None:
            try:
                self.gdrive.upload_yearly_options_parquet(sym, out_path)
            except Exception as gdrive_exc:
                logger.warning("Yearly Google Drive upload failed for %s %s: %s", sym, year, gdrive_exc)
                console.print(f"[yellow]⚠ Google Drive sync skipped for {sym} {year}: {gdrive_exc}[/yellow]")
        return out_path

    def run(self) -> None:
        """Execute the full historical download pipeline year-by-year and sync to Google Drive."""
        cfg = self.config
        console.print(
            f"[bold cyan]ThetaData Options EOD + Greeks Downloader[/bold cyan]\n"
            f"  Symbols       : {', '.join(cfg.symbols)}\n"
            f"  Date Range    : {cfg.start_date} -> {cfg.end_date}\n"
            f"  Greeks Model  : {cfg.greeks_model} (rate={cfg.rate_type}, option_nbbo={cfg.option_use_nbbo})\n"
            f"  Open Interest : {cfg.include_open_interest}\n"
            f"  Max Workers   : {cfg.max_workers}\n"
            f"  Output Dir    : {cfg.data_dir.resolve()}\n"
            f"  Google Drive  : {'Enabled (Folder ID: ' + cfg.gdrive_folder_id + ')' if self.gdrive else 'Disabled'}"
        )

        trading_days = self.get_trading_days(cfg.start_date, cfg.end_date)
        console.print(f"Found [bold]{len(trading_days):,}[/bold] trading days in range.")

        days_by_year: dict[int, list[datetime.date]] = defaultdict(list)
        for d in trading_days:
            days_by_year[d.year].append(d)

        for raw_symbol in cfg.symbols:
            symbol = validate_symbol(raw_symbol)
            console.rule(f"[bold]Downloading {symbol}[/bold]")

            if cfg.include_underlying_eod:
                self.download_underlying_eod(symbol)

            for year in sorted(days_by_year.keys()):
                year_days = days_by_year[year]
                year_dir = (
                    self.config.data_dir
                    / "raw"
                    / "options_eod"
                    / f"symbol={symbol}"
                    / f"year={year}"
                )
                empty_sentinel = year_dir / ".empty"
                is_current_year = year == nyse_today().year
                opt_root = to_option_root(symbol)
                is_remapped = symbol != opt_root

                # Validate or clear empty sentinel if parquet files exist, if it's the current year,
                # or if the symbol is remapped (e.g. BRK.B -> BRKB) where old runs created false .empty sentinels.
                if empty_sentinel.exists():
                    has_parquet = year_dir.is_dir() and any(year_dir.glob("*.parquet"))
                    sentinel_content = ""
                    try:
                        sentinel_content = empty_sentinel.read_text().strip()
                    except Exception:
                        pass
                    is_stale_remapped = is_remapped and (f"root={opt_root}" not in sentinel_content)
                    if has_parquet or is_current_year or is_stale_remapped:
                        empty_sentinel.unlink(missing_ok=True)

                existing_dates: set[datetime.date] = set()
                if cfg.skip_existing:
                    proc_path = self.yearly_parquet_path(symbol, year)
                    if proc_path.exists() and proc_path.stat().st_size > 0:
                        try:
                            df_dates = (
                                pl.scan_parquet(str(proc_path))
                                .select("trade_date")
                                .unique()
                                .collect()
                            )
                            if "trade_date" in df_dates.columns:
                                for td in df_dates["trade_date"].to_list():
                                    if isinstance(td, datetime.date):
                                        existing_dates.add(td)
                        except Exception as read_err:
                            logger.debug("Could not read trade dates from %s: %s", proc_path, read_err)

                if cfg.skip_existing and empty_sentinel.exists():
                    pending_days = []
                else:
                    pending_days = [
                        d
                        for d in year_days
                        if not (
                            cfg.skip_existing
                            and (
                                d in existing_dates
                                or (
                                    self.daily_parquet_path(symbol, d).exists()
                                    and self.daily_parquet_path(symbol, d).stat().st_size > 0
                                )
                                or self.daily_parquet_path(symbol, d).with_suffix(".empty").exists()
                            )
                        )
                    ]
                skipped_count = len(year_days) - len(pending_days)

                saved_days = 0
                empty_days = 0
                total_rows = 0
                failed_days_in_year: list[tuple[datetime.date, Exception]] = []

                if pending_days:
                    with Progress(
                        SpinnerColumn(),
                        TextColumn("[progress.description]{task.description}"),
                        BarColumn(),
                        MofNCompleteColumn(),
                        TimeElapsedColumn(),
                        TimeRemainingColumn(),
                        console=console,
                    ) as progress:
                        task_id = progress.add_task(
                            f"[green]{symbol} {year}[/green]", total=len(pending_days)
                        )
                        with concurrent.futures.ThreadPoolExecutor(
                            max_workers=cfg.max_workers
                        ) as pool:
                            futures = {
                                pool.submit(self.download_day, symbol, d): d for d in pending_days
                            }
                            for future in concurrent.futures.as_completed(futures):
                                d = futures[future]
                                try:
                                    _, rows, status = future.result()
                                    if status == "saved":
                                        saved_days += 1
                                        total_rows += rows
                                    elif status == "empty":
                                        empty_days += 1
                                except Exception as exc:
                                    console.print(f"[red]✗ Error on {symbol} {d}: {exc}[/red]")
                                    failed_days_in_year.append((d, exc))
                                finally:
                                    progress.advance(task_id)

                if failed_days_in_year:
                    # Retry failed days sequentially with exponential backoff and session refresh before aborting
                    retry_failed: list[tuple[datetime.date, Exception]] = []
                    for d, prev_exc in failed_days_in_year:
                        succeeded = False
                        for attempt in range(1, 4):
                            time.sleep(1.0 * attempt)
                            try:
                                _, rows, status = self.download_day(symbol, d)
                                if status == "saved":
                                    saved_days += 1
                                    total_rows += rows
                                elif status == "empty":
                                    empty_days += 1
                                succeeded = True
                                break
                            except Exception as retry_exc:
                                logger.warning(
                                    "Sequential retry attempt %d/3 failed for %s on %s: %s",
                                    attempt,
                                    symbol,
                                    d,
                                    retry_exc,
                                )
                                self._reauthenticate(force=True)
                                last_retry_exc = retry_exc
                        if not succeeded:
                            retry_failed.append((d, last_retry_exc))
                    failed_days_in_year = retry_failed

                if failed_days_in_year:
                    err_dates = [d.isoformat() for d, _ in failed_days_in_year]
                    raise RuntimeError(
                        f"Failed to download {len(failed_days_in_year)} session(s) for {symbol} in {year}: {err_dates}. "
                        f"Consolidation aborted to prevent permanent data holes."
                    )

                console.print(
                    f"[green]✓ {symbol} {year}:[/green] "
                    f"{saved_days:,} days saved ({total_rows:,} rows), "
                    f"{skipped_count:,} skipped, {empty_days:,} empty."
                )

                # Only write .empty sentinel if the entire past year has zero parquet files and every day returned empty
                has_parquet_now = (year_dir.is_dir() and any(year_dir.glob("*.parquet"))) or bool(existing_dates)
                if (
                    not is_current_year
                    and not has_parquet_now
                    and skipped_count == 0
                    and len(pending_days) == len(year_days)
                    and empty_days == len(year_days)
                ):
                    year_dir.mkdir(parents=True, exist_ok=True)
                    empty_sentinel.write_text(f"root={to_option_root(symbol)}\n")

                if cfg.consolidate_yearly and (saved_days > 0 or (has_parquet_now and not self.yearly_parquet_path(symbol, year).exists())):
                    self.consolidate_year(symbol, year)
