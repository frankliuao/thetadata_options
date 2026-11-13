"""Schema normalization and Parquet utilities for ThetaData Options EOD + Greeks."""

from __future__ import annotations

import datetime
import re
from pathlib import Path

import polars as pl

# One path component. Slashes and ".." are rejected so a ticker cannot escape data_dir.
_SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.]{0,9}$")


def validate_symbol(symbol: str) -> str:
    """Return a normalized ticker, or raise if it is not safe to use as a path component."""
    sym = symbol.strip().upper()
    if not _SYMBOL_RE.fullmatch(sym):
        raise ValueError(
            f"Invalid ticker {symbol!r}. Expected 1-10 characters using A-Z, digits, or '.'."
        )
    return sym

# Columns returned by ThetaData option_history_greeks_eod / option_history_binomial_greeks_eod
# Note: ThetaData v3 returns some Greeks (like theta, vanna, color, ultima, epsilon, dual_delta)
# as strings in certain responses; we cast all numeric/greek columns to Float64.
FLOAT_COLUMNS: tuple[str, ...] = (
    "strike",
    "open",
    "high",
    "low",
    "close",
    "bid",
    "ask",
    "underlying_price",
    "implied_vol",
    "iv_error",
    # 1st order Greeks
    "delta",
    "theta",
    "vega",
    "vega_per_pct",
    "rho",
    "rho_per_pct",
    "epsilon",
    "lambda",
    # 2nd order Greeks
    "gamma",
    "vanna",
    "charm",
    "vomma",
    "veta",
    "vera",
    "dual_delta",
    "dual_gamma",
    # 3rd order Greeks & BSM terms
    "speed",
    "zomma",
    "color",
    "ultima",
    "d1",
    "d2",
)

INT_COLUMNS: tuple[str, ...] = (
    "volume",
    "count",
    "bid_size",
    "bid_exchange",
    "bid_condition",
    "ask_size",
    "ask_exchange",
    "ask_condition",
    "open_interest",
    "open_interest_prior_close",
)

_OPTION_KEYS: tuple[str, ...] = ("trade_date", "expiration", "strike", "right")


def date_expr(column: str, dtype: pl.DataType) -> pl.Expr:
    """Build an expression that parses `column` as a calendar date.

    Strings may be ``YYYY-MM-DD`` or ``YYYYMMDD``. Integers and whole-number
    floats are ``YYYYMMDD``. They are not days since the Unix epoch, which is
    what ``cast(pl.Date)`` would assume.
    """
    if dtype == pl.Date:
        return pl.col(column)
    if dtype in (pl.Utf8, pl.String):
        return (
            pl.col(column)
            .str.to_date(format="%Y-%m-%d", strict=False)
            .fill_null(pl.col(column).str.to_date(format="%Y%m%d", strict=False))
        )
    if dtype.is_integer() or dtype.is_float():
        return (
            pl.col(column)
            .cast(pl.Int64, strict=False)
            .cast(pl.Utf8)
            .str.to_date(format="%Y%m%d", strict=False)
        )
    if dtype == pl.Datetime or dtype.base_type() == pl.Datetime:
        return pl.col(column).dt.date()
    return pl.col(column).cast(pl.Date, strict=False)


def normalize_options_df(
    df: pl.DataFrame,
    trade_date: datetime.date | None = None,
) -> pl.DataFrame:
    """Normalize column types and compute standard backtest helper columns.

    Adds/normalizes:
    - `trade_date` (pl.Date): Trading date of the EOD record
    - `expiration` (pl.Date): Option expiration date
    - `right` (pl.Utf8): Normalized lowercase ('call' or 'put')
    - `dte` (pl.Int32): Calendar days to expiration (`expiration - trade_date`)
    - `mid` (pl.Float64): Closing NBBO midpoint `(bid + ask) / 2.0`
    - `spread` (pl.Float64): Closing NBBO spread `ask - bid`
    - All price and Greek columns cast cleanly to `Float64`
    """
    if df.is_empty():
        return df

    exprs: list[pl.Expr] = []

    # 1. Normalize expiration to Date
    if "expiration" in df.columns:
        exprs.append(date_expr("expiration", df.schema["expiration"]).alias("expiration"))

    # 2. Normalize right ('call' / 'put')
    if "right" in df.columns:
        exprs.append(
            pl.col("right")
            .cast(pl.Utf8)
            .str.to_lowercase()
            .replace({"c": "call", "p": "put"})
            .alias("right")
        )

    # 3. Extract or set trade_date
    if trade_date is not None:
        exprs.append(pl.lit(trade_date).cast(pl.Date).alias("trade_date"))
    elif "trade_date" in df.columns:
        exprs.append(date_expr("trade_date", df.schema["trade_date"]).alias("trade_date"))
    elif "timestamp" in df.columns:
        exprs.append(pl.col("timestamp").dt.date().alias("trade_date"))
    elif "created" in df.columns:
        exprs.append(pl.col("created").dt.date().alias("trade_date"))

    # 4. Cast float columns (including string-encoded Greeks)
    for col in FLOAT_COLUMNS:
        if col in df.columns:
            exprs.append(pl.col(col).cast(pl.Float64, strict=False).alias(col))

    # 5. Cast int columns
    for col in INT_COLUMNS:
        if col in df.columns:
            exprs.append(pl.col(col).cast(pl.Int64, strict=False).alias(col))

    df = df.with_columns(exprs)

    # 6. Derived columns useful for options backtesting: dte, mid, spread, scaled Greeks, OI alias
    derived: list[pl.Expr] = []
    if "expiration" in df.columns and "trade_date" in df.columns:
        derived.append(
            (pl.col("expiration") - pl.col("trade_date")).dt.total_days().cast(pl.Int32).alias("dte")
        )
    if "bid" in df.columns and "ask" in df.columns:
        derived.append(((pl.col("bid") + pl.col("ask")) * 0.5).alias("mid"))
        derived.append((pl.col("ask") - pl.col("bid")).alias("spread"))
    if "vega" in df.columns and "vega_per_pct" not in df.columns:
        # ThetaData raw vega is the mathematical Black-Scholes derivative dV/dSigma (100% vol change).
        # Standard convention measures sensitivity per 1 percentage point move in IV (dV / 100).
        derived.append((pl.col("vega") / 100.0).alias("vega_per_pct"))
    if "rho" in df.columns and "rho_per_pct" not in df.columns:
        # ThetaData raw rho is mathematical dV/dr (100% interest rate shift).
        # Standard convention measures sensitivity per 1 percentage point move in rate (dV / 100).
        derived.append((pl.col("rho") / 100.0).alias("rho_per_pct"))
    if "open_interest" in df.columns and "open_interest_prior_close" not in df.columns:
        # OPRA daily OI is published at ~06:30 ET representing the prior session's close.
        derived.append(pl.col("open_interest").alias("open_interest_prior_close"))

    if derived:
        df = df.with_columns(derived)

    # 7. Order columns logically: identifiers first, then prices/quotes, then Greeks
    preferred_prefix = [
        "trade_date",
        "symbol",
        "expiration",
        "dte",
        "strike",
        "right",
        "underlying_price",
        "open",
        "high",
        "low",
        "close",
        "bid",
        "ask",
        "mid",
        "spread",
        "volume",
        "count",
        "open_interest",
        "open_interest_prior_close",
        "implied_vol",
        "delta",
        "gamma",
        "theta",
        "vega",
        "vega_per_pct",
        "rho",
        "rho_per_pct",
    ]
    ordered = [c for c in preferred_prefix if c in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    out = df.select(ordered + remaining)
    sort_cols = [c for c in ("expiration", "strike", "right") if c in out.columns]
    if sort_cols:
        out = out.sort(sort_cols)
    return out


def merge_option_frames(frames: list[pl.DataFrame]) -> pl.DataFrame:
    """Stack option chains and keep the last non-null value of each column.

    Rows are keyed by trade date, expiration, strike, and right. A later row
    with a null ``open_interest`` must not erase open interest already stored
    for that contract, while a later non-null bid still replaces the older bid.
    """
    usable = [frame for frame in frames if frame is not None and not frame.is_empty()]
    if not usable:
        return pl.DataFrame()
    df = pl.concat(usable, how="diagonal_relaxed")
    keys = [key for key in _OPTION_KEYS if key in df.columns]
    if not keys:
        return df
    value_cols = [col for col in df.columns if col not in keys]
    if value_cols:
        df = df.group_by(keys, maintain_order=True).agg(
            [pl.col(col).drop_nulls().last().alias(col) for col in value_cols]
        )
    else:
        df = df.unique(subset=keys, keep="last")
    return df.sort(keys)


def merge_underlying_frames(existing: pl.DataFrame, fresh: pl.DataFrame) -> pl.DataFrame:
    """Union underlying EOD sessions. Rows in `fresh` replace the same session.

    ``created`` is the ThetaData EOD report timestamp, one row per session.
    Refusing to merge when that column is absent avoids collapsing every row
    into a single record or replacing a longer history with a short window.
    """
    if existing.is_empty():
        return fresh
    if fresh.is_empty():
        return existing
    df = pl.concat([existing, fresh], how="diagonal_relaxed")
    if "created" not in df.columns:
        raise ValueError(
            "Underlying EOD frame has no 'created' column; refusing to replace saved history."
        )
    df = df.with_columns(date_expr("created", df.schema["created"]).alias("_session"))
    df = df.unique(subset=["_session"], keep="last").drop("_session")
    return df.sort("created")


def write_parquet_atomic(df: pl.DataFrame, path: str | Path, compression: str = "zstd") -> None:
    """Write a Polars DataFrame to Parquet atomically via a temporary file."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(".parquet.tmp")
    df.write_parquet(tmp_path, compression=compression, statistics=True)
    tmp_path.replace(target)
