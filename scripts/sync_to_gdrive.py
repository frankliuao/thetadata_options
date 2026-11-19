"""Synchronize updated Parquet files and universe files to Google Drive respecting the folder hierarchy."""

from __future__ import annotations

import argparse
import concurrent.futures
import os
from pathlib import Path
from rich.console import Console
from dotenv import load_dotenv

from options_data.gdrive_sync import GoogleDriveSync

console = Console()


def upload_single_file(sync: GoogleDriveSync, symbol: str, local_file: Path) -> tuple[Path, bool, str]:
    try:
        sync.upload_yearly_options_parquet(symbol, local_file)
        return local_file, True, "uploaded"
    except Exception as exc:
        return local_file, False, str(exc)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Sync processed options Parquet files to Google Drive.")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to sync (default: scan processed dir).")
    parser.add_argument("--years", type=int, nargs="+", help="Specific years to sync (e.g. 2025 2026).")
    parser.add_argument("--all", action="store_true", help="Sync all files in data/processed/options_eod.")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Concurrent upload threads (default: 4).")
    args = parser.parse_args(argv)

    sync = GoogleDriveSync()
    if not sync.root_folder_id:
        console.print("[red]Error: GDRIVE_FOLDER_ID not set in .env[/red]")
        return 1

    # 1. Sync sp500_universe.csv to root
    universe_csv = Path("config/sp500_universe.csv")
    if universe_csv.exists():
        console.print("[bold cyan]Syncing sp500_universe.csv to Google Drive root...[/bold cyan]")
        try:
            fid = sync.upload_or_update_file(universe_csv, sync.root_folder_id, mime_type="text/csv")
            console.print(f"  [bold green]✓ Uploaded sp500_universe.csv (ID: {fid})[/bold green]")
        except Exception as exc:
            console.print(f"[yellow]⚠ Failed to upload sp500_universe.csv: {exc}[/yellow]")

    # 2. Gather files to upload
    root = Path("data/processed/options_eod")
    files_to_upload: list[tuple[str, Path]] = []

    target_syms = [s.upper() for s in args.symbols] if args.symbols else None
    target_years = set(args.years) if args.years else None

    for sym_dir in sorted(root.glob("symbol=*")):
        sym = sym_dir.name.replace("symbol=", "").upper()
        if target_syms and sym not in target_syms:
            continue
        for pf in sorted(sym_dir.glob("*.parquet")):
            if target_years:
                try:
                    yr = int(pf.stem.split("_")[-1])
                    if yr not in target_years:
                        continue
                except ValueError:
                    pass
            files_to_upload.append((sym, pf))

    console.print(f"[bold cyan]Found {len(files_to_upload)} files to sync with {args.workers} workers...[/bold cyan]")

    succeeded = 0
    failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(upload_single_file, sync, sym, pf): (sym, pf)
            for sym, pf in files_to_upload
        }
        for future in concurrent.futures.as_completed(futures):
            pf, ok, msg = future.result()
            if ok:
                succeeded += 1
            else:
                failed += 1
                console.print(f"[red]Failed {pf.name}: {msg}[/red]")

    console.print(
        f"[bold green]Google Drive Sync Complete: {succeeded} uploaded/updated, {failed} failed.[/bold green]"
    )
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
