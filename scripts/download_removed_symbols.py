"""Download the 26 missing Removed symbols from 2016 to 2026-11-13 with unlimited DTE."""

from __future__ import annotations

import datetime
from pathlib import Path
from rich.console import Console

from options_data.downloader import ThetaOptionsDownloader, DownloadConfig

console = Console()

REMOVED_26 = [
    "AMG",
    "AN",
    "AYI",
    "BHF",
    "CNX",
    "FLR",
    "FOSL",
    "GME",
    "GT",
    "JEF",
    "MAC",
    "MAT",
    "MUR",
    "NAVI",
    "NKTR",
    "OI",
    "PBI",
    "R",
    "RIG",
    "RRC",
    "SIG",
    "TDC",
    "THC",
    "TNL",
    "TRIP",
    "URBN",
]


def main() -> None:
    console.print(
        f"[bold cyan]Starting download for {len(REMOVED_26)} removed symbols (2016-01-01 to 2026-11-13)...[/bold cyan]"
    )
    cfg = DownloadConfig(
        symbols=REMOVED_26,
        start_date=datetime.date(2016, 1, 1),
        end_date=datetime.date(2026, 11, 13),
        max_dte=150,
        max_workers=3,
        sync_to_gdrive=False,
    )
    dl = ThetaOptionsDownloader(cfg)
    dl.run()
    console.print("[bold green]All 26 removed symbols downloaded successfully![/bold green]")


if __name__ == "__main__":
    main()
