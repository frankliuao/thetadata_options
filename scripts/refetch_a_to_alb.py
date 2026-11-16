"""Refetch ALB 2025-2026 and A..ALB post-2026-10-29 with unlimited DTE (max_dte=None)."""

from __future__ import annotations

import datetime
from pathlib import Path
import polars as pl
from rich.console import Console

from options_data.downloader import ThetaOptionsDownloader, DownloadConfig
from options_data.schema import write_parquet_atomic
from options_data.universe import load_universe_symbols

console = Console()


def run_refetch() -> None:
    active = sorted(load_universe_symbols(mode="active"))
    a_to_alb = [s for s in active if s <= "ALB"]
    console.print(f"[bold green]Starting refetch for {len(a_to_alb)} symbols (A to ALB)...[/bold green]")

    end_date = datetime.date(2026, 11, 13)

    # 1. Clean ALB 2025 and 2026 entirely
    console.print("[yellow]Cleaning ALB 2025 and 2026 processed and raw data...[/yellow]")
    alb_proc_dir = Path("data/processed/options_eod/symbol=ALB")
    for yr in [2025, 2026]:
        p = alb_proc_dir / f"ALB_{yr}.parquet"
        if p.exists():
            p.unlink()
        raw_yr_dir = Path(f"data/raw/options_eod/symbol=ALB/year={yr}")
        if raw_yr_dir.exists():
            for rf in raw_yr_dir.glob("*.parquet"):
                rf.unlink()
            for ef in raw_yr_dir.glob("*.empty"):
                ef.unlink()

    # 2. Clean post-2026-10-29 for the other 23 symbols
    cutoff = datetime.date(2026, 10, 29)
    for sym in a_to_alb:
        if sym == "ALB":
            continue
        p26 = Path(f"data/processed/options_eod/symbol={sym}/{sym}_2026.parquet")
        if p26.exists():
            df = pl.read_parquet(p26)
            before_len = len(df)
            df_filtered = df.filter(pl.col("trade_date") < cutoff)
            after_len = len(df_filtered)
            write_parquet_atomic(df_filtered, p26)
            console.print(f"  {sym} 2026: pruned {before_len - after_len} post-Oct29 contracts (remaining: {after_len})")

        raw_yr_dir = Path(f"data/raw/options_eod/symbol={sym}/year=2026")
        if raw_yr_dir.exists():
            for rf in raw_yr_dir.glob(f"{sym}_*.parquet"):
                stem = rf.stem
                d_str = stem.replace(f"{sym}_", "")
                try:
                    d = datetime.date.fromisoformat(d_str)
                    if d >= cutoff:
                        rf.unlink()
                except ValueError:
                    pass
            for ef in raw_yr_dir.glob(f"{sym}_*.empty"):
                stem = ef.stem
                d_str = stem.replace(f"{sym}_", "")
                try:
                    d = datetime.date.fromisoformat(d_str)
                    if d >= cutoff:
                        ef.unlink()
                except ValueError:
                    pass

    # 3. Refetch ALB from 2025-01-01 to 2026-11-13
    console.print("[bold cyan]Fetching ALB 2025-01-01 -> 2026-11-13 (max_dte=None)...[/bold cyan]")
    cfg_alb = DownloadConfig(
        symbols=["ALB"],
        start_date=datetime.date(2025, 1, 1),
        end_date=end_date,
        max_dte=None,
        max_workers=4,
        sync_to_gdrive=False,
    )
    dl_alb = ThetaOptionsDownloader(cfg_alb)
    dl_alb.run()
    console.print("[bold green]ALB 2025-2026 download complete![/bold green]")

    # 4. Refetch the other 23 symbols from 2026-10-29 to 2026-11-13
    other_syms = [s for s in a_to_alb if s != "ALB"]
    console.print(f"[bold cyan]Fetching {len(other_syms)} symbols from 2026-10-29 -> 2026-11-13 (max_dte=None)...[/bold cyan]")
    cfg_others = DownloadConfig(
        symbols=other_syms,
        start_date=cutoff,
        end_date=end_date,
        max_dte=None,
        max_workers=4,
        sync_to_gdrive=False,
    )
    dl_others = ThetaOptionsDownloader(cfg_others)
    dl_others.run()

    console.print("[bold green]A to ALB refetch completely done![/bold green]")


if __name__ == "__main__":
    run_refetch()
