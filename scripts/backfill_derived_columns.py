"""Backfill vega_per_pct, rho_per_pct, and open_interest_prior_close across all local Parquet files."""

from __future__ import annotations

import concurrent.futures
from pathlib import Path
import polars as pl
from rich.console import Console

from options_data.schema import normalize_options_df, write_parquet_atomic

console = Console()


def process_file(file_path: Path) -> tuple[Path, bool, str]:
    try:
        schema = pl.read_parquet_schema(file_path)
        cols = set(schema.keys())

        needs_update = False
        if "vega" in cols and "vega_per_pct" not in cols:
            needs_update = True
        elif "rho" in cols and "rho_per_pct" not in cols:
            needs_update = True
        elif "open_interest" in cols and "open_interest_prior_close" not in cols:
            needs_update = True
        elif file_path.name.endswith("_2026.parquet") and "vega" in cols and "vega_per_pct" in cols:
            # Check if there are nulls before Oct 29
            sample = pl.read_parquet(
                file_path,
                columns=["vega", "vega_per_pct"],
                n_rows=5000,
            )
            bad = sample.filter(pl.col("vega").is_not_null() & pl.col("vega_per_pct").is_null())
            if len(bad) > 0:
                needs_update = True

        if not needs_update:
            return file_path, False, "ok"

        df = pl.read_parquet(file_path)
        df_norm = normalize_options_df(df)
        write_parquet_atomic(df_norm, file_path)
        return file_path, True, "updated"
    except Exception as exc:
        return file_path, False, f"error: {exc}"


def main() -> None:
    root = Path("data/processed/options_eod")
    all_files = sorted(root.glob("**/*.parquet"))
    console.print(f"[bold cyan]Scanning {len(all_files)} Parquet files for derived columns backfill...[/bold cyan]")

    workers = 4
    updated_count = 0
    error_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_file, p): p for p in all_files}
        for future in concurrent.futures.as_completed(futures):
            path, updated, msg = future.result()
            if updated:
                updated_count += 1
            elif msg.startswith("error"):
                error_count += 1
                console.print(f"[red]Error on {path}: {msg}[/red]")

    console.print(
        f"[bold green]Done! Total files: {len(all_files)}, Updated: {updated_count}, Errors: {error_count}[/bold green]"
    )


if __name__ == "__main__":
    main()
