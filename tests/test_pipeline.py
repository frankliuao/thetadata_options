"""Offline checks for the options data pipeline. No ThetaData credentials."""

from __future__ import annotations

import datetime
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from options_data.cli import main as download_main
from options_data.downloader import (
    DownloadConfig,
    ThetaOptionsDownloader,
    load_merged_options_year,
    nyse_today,
    to_option_root,
    validate_symbol,
)
from options_data.gdrive_sync import get_gdrive_base_cmd
from options_data.loader import OptionsDataLoader
from options_data.schema import date_expr, merge_underlying_frames, normalize_options_df
from options_data.universe import load_universe_symbols
from options_data.updater import IncrementalOptionsUpdater, find_latest_downloaded_date


class SchemaTests(unittest.TestCase):
    def test_compact_and_integer_expiration_drive_dte(self) -> None:
        trade_date = datetime.date(2024, 11, 15)
        frame = pl.DataFrame(
            {
                "expiration": ["20241220", "2025-01-17"],
                "strike": [100.0, 110.0],
                "right": ["PUT", "CALL"],
                "bid": [1.0, 2.0],
                "ask": [1.4, 2.2],
                "theta": ["-0.215", "0.010"],
            }
        )
        out = normalize_options_df(frame, trade_date=trade_date)
        self.assertEqual(out["expiration"].to_list(), [datetime.date(2024, 12, 20), datetime.date(2025, 1, 17)])
        self.assertEqual(out["dte"].to_list(), [35, 63])
        self.assertEqual(out["right"].to_list(), ["put", "call"])
        self.assertAlmostEqual(out["mid"][0], 1.2)
        self.assertAlmostEqual(out["theta"][0], -0.215)

        yyyymmdd = pl.DataFrame(
            {
                "expiration": [20241220],
                "strike": [100.0],
                "right": ["put"],
                "bid": [1.0],
                "ask": [1.2],
            }
        )
        parsed = normalize_options_df(yyyymmdd, trade_date=trade_date)
        self.assertEqual(parsed["expiration"][0], datetime.date(2024, 12, 20))
        self.assertEqual(parsed["dte"][0], 35)

    def test_integer_calendar_date_is_not_an_epoch_day(self) -> None:
        frame = pl.DataFrame({"date": [20241225]})
        parsed = frame.with_columns(date_expr("date", frame.schema["date"]).alias("date"))
        self.assertEqual(parsed["date"][0], datetime.date(2024, 12, 25))

    def test_partial_frame_without_contract_keys_does_not_crash(self) -> None:
        out = normalize_options_df(
            pl.DataFrame({"bid": [1.0], "ask": [1.2]}),
            trade_date=datetime.date(2024, 1, 2),
        )
        self.assertAlmostEqual(out["mid"][0], 1.1)
        self.assertEqual(out["trade_date"][0], datetime.date(2024, 1, 2))

    def test_scaled_greeks_and_oi_alias(self) -> None:
        trade_date = datetime.date(2024, 11, 15)
        frame = pl.DataFrame(
            {
                "expiration": ["2024-12-20", "2024-12-20"],
                "strike": [100.0, 100.0],
                "right": ["C", "P"],
                "vega": [2.635, 1.50],
                "rho": [0.4387, -0.210],
                "open_interest": [500, 1200],
            }
        )
        out = normalize_options_df(frame, trade_date=trade_date)
        self.assertEqual(out["right"].to_list(), ["call", "put"])
        self.assertAlmostEqual(out["vega_per_pct"][0], 0.02635)
        self.assertAlmostEqual(out["rho_per_pct"][0], 0.004387)
        self.assertEqual(out["open_interest_prior_close"].to_list(), [500, 1200])


class MergeTests(unittest.TestCase):
    def test_year_merge_keeps_prior_days_and_open_interest(self) -> None:
        day1 = datetime.date(2024, 1, 2)
        day2 = datetime.date(2024, 1, 3)
        day3 = datetime.date(2024, 1, 4)
        expiration = datetime.date(2024, 2, 16)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            processed = root / "processed" / "options_eod" / "symbol=SPY"
            raw = root / "raw" / "options_eod" / "symbol=SPY" / "year=2024"
            processed.mkdir(parents=True)
            raw.mkdir(parents=True)
            pl.DataFrame(
                {
                    "trade_date": [day1, day2],
                    "expiration": [expiration, expiration],
                    "strike": [100.0, 100.0],
                    "right": ["put", "put"],
                    "bid": [1.0, 1.1],
                    "open_interest": [50, 60],
                }
            ).write_parquet(processed / "SPY_2024.parquet")
            pl.DataFrame(
                {
                    "trade_date": [day2, day3],
                    "expiration": [expiration, expiration],
                    "strike": [100.0, 100.0],
                    "right": ["put", "put"],
                    "bid": [1.4, 1.5],
                    "open_interest": [None, 70],
                }
            ).write_parquet(raw / "SPY_2024-01-04.parquet")

            merged = load_merged_options_year(root, "SPY", 2024)

        self.assertIsNotNone(merged)
        assert merged is not None
        self.assertEqual(merged["trade_date"].to_list(), [day1, day2, day3])
        day2_row = merged.filter(pl.col("trade_date") == day2)
        self.assertAlmostEqual(day2_row["bid"][0], 1.4)
        self.assertEqual(day2_row["open_interest"][0], 60)

    def test_underlying_merge_keeps_sessions_outside_the_new_window(self) -> None:
        first = datetime.datetime(2024, 1, 2, 17, 15, tzinfo=ZoneInfo("America/New_York"))
        second = datetime.datetime(2024, 1, 3, 17, 15, tzinfo=ZoneInfo("America/New_York"))
        existing = pl.DataFrame({"created": [first, second], "close": [10.0, 11.0]})
        fresh = pl.DataFrame({"created": [second], "close": [12.5]})
        merged = merge_underlying_frames(existing, fresh)
        self.assertEqual(merged.height, 2)
        self.assertAlmostEqual(merged.sort("created")["close"][0], 10.0)
        self.assertAlmostEqual(merged.sort("created")["close"][1], 12.5)


class SymbolAndCalendarTests(unittest.TestCase):
    def test_path_traversal_ticker_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            validate_symbol("../../../../../outside")
        with self.assertRaises(ValueError):
            validate_symbol("SPY/../X")
        self.assertEqual(validate_symbol("brk.b"), "BRK.B")

    def test_checked_in_universe_matches_documented_counts(self) -> None:
        current = load_universe_symbols(mode="current")
        removed = load_universe_symbols(mode="removed_active")
        active = load_universe_symbols(mode="active")
        everything = load_universe_symbols(mode="all", include_historical_tickers=True)
        self.assertEqual(len(current), 503)
        self.assertEqual(len(removed), 70)
        self.assertEqual(len(active), 573)
        self.assertEqual(len(everything), 656)
        self.assertIn("OKTA", current)
        self.assertIn("BRK.B", current)
        self.assertIn("BF.B", current)
        self.assertIn("GPS", everything)
        self.assertIn("GAP", everything)
        self.assertIn("CDAY", everything)
        self.assertIn("DAY", everything)
        all_default = load_universe_symbols(mode="all", include_historical_tickers=False)
        self.assertIn("CDAY", all_default)
        self.assertIn("DAY", all_default)
        for symbol in everything:
            validate_symbol(symbol)

    def test_nyse_today_is_the_new_york_session_date(self) -> None:
        session = nyse_today()
        local = datetime.date.today()
        self.assertIsInstance(session, datetime.date)
        self.assertLessEqual(abs((session - local).days), 1)
        self.assertEqual(
            session,
            datetime.datetime.now(ZoneInfo("America/New_York")).date(),
        )

    def test_gdrive_command_uses_the_current_home(self) -> None:
        command, env = get_gdrive_base_cmd()
        self.assertIn(str(Path.home() / ".gemini" / "antigravity" / "bin"), env["PATH"])

    def test_to_option_root_converts_class_shares(self) -> None:
        self.assertEqual(to_option_root("BRK.B"), "BRKB")
        self.assertEqual(to_option_root("BF.B"), "BFB")
        self.assertEqual(to_option_root("AAPL"), "AAPL")

    def test_gdrive_sync_has_thread_lock(self) -> None:
        from options_data.gdrive_sync import GoogleDriveSync
        sync = GoogleDriveSync()
        self.assertTrue(hasattr(sync, "_lock"))
        with sync._lock:
            self.assertEqual(sync.root_folder_id, sync.root_folder_id)


class CatchUpTests(unittest.TestCase):
    def test_latest_date_considers_processed_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw" / "options_eod" / "symbol=SPY" / "year=2024"
            processed = root / "processed" / "options_eod" / "symbol=SPY"
            raw.mkdir(parents=True)
            processed.mkdir(parents=True)
            pl.DataFrame({"trade_date": [datetime.date(2024, 11, 15)]}).write_parquet(
                raw / "SPY_2024-11-15.parquet"
            )
            pl.DataFrame({"trade_date": [datetime.date(2024, 12, 20)]}).write_parquet(
                processed / "SPY_2024.parquet"
            )
            self.assertEqual(
                find_latest_downloaded_date(root, "SPY"),
                datetime.date(2024, 12, 20),
            )

    def test_one_failed_day_stops_that_symbol_and_continues_the_next(self) -> None:
        days = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]

        class FakeDownloader:
            def __init__(self) -> None:
                self.gdrive = None
                self.consolidated: list[tuple[str, int]] = []
                self.seen: list[tuple[str, datetime.date]] = []

            def get_trading_days(self, start: datetime.date, end: datetime.date) -> list[datetime.date]:
                return [day for day in days if start <= day <= end]

            def download_day(self, symbol: str, trade_date: datetime.date) -> tuple[datetime.date, int, str]:
                self.seen.append((symbol, trade_date))
                if trade_date == days[0]:
                    raise RuntimeError("transient failure")
                return (trade_date, 3, "saved")

            def consolidate_year(self, symbol: str, year: int) -> None:
                self.consolidated.append((symbol, year))

        fake = FakeDownloader()
        updater = IncrementalOptionsUpdater(
            config=DownloadConfig(
                symbols=["SPY", "AAPL"],
                start_date=days[0],
                end_date=days[-1],
                sync_to_gdrive=False,
                data_dir=Path("/tmp/options-review-unused"),
            ),
            downloader=fake,  # type: ignore[arg-type]
        )
        failed = updater.run_update(target_end_date=days[-1], forced_since=days[0])
        self.assertEqual(failed, 2)
        self.assertEqual(sorted(fake.seen), [("AAPL", days[0]), ("SPY", days[0])])
        self.assertEqual(fake.consolidated, [])

    def test_mid_range_failure_is_retried_on_the_next_run(self) -> None:
        days = [
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 3),
            datetime.date(2024, 1, 4),
        ]

        class FakeDownloader:
            def __init__(self, root: Path) -> None:
                self.root = root
                self.gdrive = None
                self.seen: list[datetime.date] = []
                self.failures_left = 1

            def get_trading_days(self, start: datetime.date, end: datetime.date) -> list[datetime.date]:
                return [day for day in days if start <= day <= end]

            def download_day(self, symbol: str, trade_date: datetime.date) -> tuple[datetime.date, int, str]:
                self.seen.append(trade_date)
                if trade_date == days[1] and self.failures_left:
                    self.failures_left -= 1
                    raise RuntimeError("mid-range failure")
                out = (
                    self.root
                    / "raw"
                    / "options_eod"
                    / f"symbol={symbol}"
                    / f"year={trade_date.year}"
                    / f"{symbol}_{trade_date.isoformat()}.parquet"
                )
                out.parent.mkdir(parents=True, exist_ok=True)
                pl.DataFrame({"trade_date": [trade_date]}).write_parquet(out)
                return (trade_date, 1, "saved")

            def consolidate_year(self, symbol: str, year: int) -> None:
                raw = self.root / "raw" / "options_eod" / f"symbol={symbol}" / f"year={year}"
                frames = [pl.read_parquet(path) for path in sorted(raw.glob("*.parquet"))]
                if not frames:
                    return
                out = (
                    self.root
                    / "processed"
                    / "options_eod"
                    / f"symbol={symbol}"
                    / f"{symbol}_{year}.parquet"
                )
                out.parent.mkdir(parents=True, exist_ok=True)
                pl.concat(frames).write_parquet(out)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake = FakeDownloader(root)
            updater = IncrementalOptionsUpdater(
                config=DownloadConfig(
                    symbols=["SPY"],
                    start_date=days[0],
                    end_date=days[-1],
                    sync_to_gdrive=False,
                    data_dir=root,
                    consolidate_yearly=True,
                ),
                downloader=fake,  # type: ignore[arg-type]
            )
            failed = updater.run_update(target_end_date=days[-1], forced_since=days[0])
            self.assertEqual(failed, 1)
            self.assertEqual(fake.seen, [days[0], days[1]])
            self.assertEqual(find_latest_downloaded_date(root, "SPY"), days[0])
            self.assertFalse(
                (
                    root
                    / "raw"
                    / "options_eod"
                    / "symbol=SPY"
                    / "year=2024"
                    / f"SPY_{days[2].isoformat()}.parquet"
                ).exists()
            )

            fake.seen.clear()
            failed_again = updater.run_update(target_end_date=days[-1], forced_since=None)
            self.assertEqual(failed_again, 0)
            self.assertEqual(fake.seen, [days[1], days[2]])
            self.assertEqual(find_latest_downloaded_date(root, "SPY"), days[2])

    def test_multithreaded_updater_runs_concurrently(self) -> None:
        days = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3)]

        class FakeDownloader:
            def __init__(self) -> None:
                self.gdrive = None
                self.seen: list[tuple[str, datetime.date]] = []

            def get_trading_days(self, start: datetime.date, end: datetime.date) -> list[datetime.date]:
                return [day for day in days if start <= day <= end]

            def download_day(self, symbol: str, trade_date: datetime.date) -> tuple[datetime.date, int, str]:
                self.seen.append((symbol, trade_date))
                return (trade_date, 10, "saved")

            def consolidate_year(self, symbol: str, year: int) -> None:
                pass

        fake = FakeDownloader()
        updater = IncrementalOptionsUpdater(
            config=DownloadConfig(
                symbols=["SPY", "AAPL"],
                start_date=days[0],
                end_date=days[-1],
                sync_to_gdrive=False,
                max_workers=2,
                consolidate_yearly=False,
            ),
            downloader=fake,  # type: ignore[arg-type]
        )
        failed = updater.run_update(target_end_date=days[-1], forced_since=days[0])
        self.assertEqual(failed, 0)
        self.assertEqual(len(fake.seen), 4)
        symbols_seen = {s for s, _ in fake.seen}
        self.assertEqual(symbols_seen, {"SPY", "AAPL"})


class MockCliTests(unittest.TestCase):
    def test_mock_pipeline_exits_zero(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(download_main(["--mock-test", "--data-dir", tmp]), 0)

    def test_normalize_respects_explicit_trade_date_over_timestamp(self) -> None:
        df = pl.DataFrame({
            "symbol": ["SPY"],
            "expiration": [datetime.date(2024, 12, 20)],
            "strike": [500.0],
            "right": ["C"],
            "timestamp": [datetime.datetime(2024, 1, 1, 23, 30, 0)],
            "bid": [10.0],
            "ask": [11.0],
            "open_interest": [100],
        })
        normalized = normalize_options_df(df, trade_date=datetime.date(2024, 1, 2))
        self.assertEqual(normalized["trade_date"][0], datetime.date(2024, 1, 2))
        self.assertEqual(normalized["open_interest_prior_close"][0], 100)

        # Re-normalizing after replacing dedicated OI should update open_interest_prior_close
        deduped = normalized.drop(["open_interest", "open_interest_prior_close"]).with_columns(
            pl.lit(500).alias("open_interest")
        )
        renormalized = normalize_options_df(deduped, trade_date=datetime.date(2024, 1, 2))
        self.assertEqual(renormalized["open_interest"][0], 500)
        self.assertEqual(renormalized["open_interest_prior_close"][0], 500)

        # Fallback when trade_date is None should parse existing trade_date column without TypeError
        fallback = normalize_options_df(renormalized, trade_date=None)
        self.assertEqual(fallback["trade_date"][0], datetime.date(2024, 1, 2))

    def test_loader_unions_processed_with_newer_raw_sessions(self) -> None:
        from options_data.loader import OptionsDataLoader

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            sym = "SPY"
            proc_dir = data_dir / "processed" / "options_eod" / f"symbol={sym}"
            raw_dir = data_dir / "raw" / "options_eod" / f"symbol={sym}" / "year=2024"
            proc_dir.mkdir(parents=True)
            raw_dir.mkdir(parents=True)

            # Year 2023 in processed
            pl.DataFrame({
                "trade_date": [datetime.date(2023, 12, 15)],
                "symbol": [sym],
                "expiration": [datetime.date(2024, 1, 19)],
                "strike": [450.0],
                "right": ["call"],
            }).write_parquet(proc_dir / f"{sym}_2023.parquet")

            # Year 2024 in raw (unconsolidated)
            pl.DataFrame({
                "trade_date": [datetime.date(2024, 1, 15)],
                "symbol": [sym],
                "expiration": [datetime.date(2024, 2, 16)],
                "strike": [470.0],
                "right": ["call"],
            }).write_parquet(raw_dir / f"{sym}_2024-01-15.parquet")

            loader = OptionsDataLoader(data_dir=data_dir)
            lf = loader.scan_symbol(sym)
            df = lf.collect()
            self.assertEqual(df.height, 2)
            self.assertEqual(sorted(df["trade_date"].to_list()), [datetime.date(2023, 12, 15), datetime.date(2024, 1, 15)])

    def test_updater_heals_internal_gaps(self) -> None:
        days = [
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 3),
            datetime.date(2024, 1, 4),
        ]

        class FakeDownloader:
            def __init__(self, root: Path) -> None:
                self.root = root
                self.gdrive = None
                self.seen: list[datetime.date] = []

            def get_trading_days(self, start: datetime.date, end: datetime.date) -> list[datetime.date]:
                return [day for day in days if start <= day <= end]

            def download_day(self, symbol: str, trade_date: datetime.date) -> tuple[datetime.date, int, str]:
                self.seen.append(trade_date)
                out = (
                    self.root
                    / "raw"
                    / "options_eod"
                    / f"symbol={symbol}"
                    / f"year={trade_date.year}"
                    / f"{symbol}_{trade_date.isoformat()}.parquet"
                )
                out.parent.mkdir(parents=True, exist_ok=True)
                pl.DataFrame({"trade_date": [trade_date]}).write_parquet(out)
                return (trade_date, 1, "saved")

            def consolidate_year(self, symbol: str, year: int) -> None:
                pass

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            # Create files for day 0 and day 2, leaving day 1 as an internal gap
            raw_dir = root / "raw" / "options_eod" / "symbol=SPY" / "year=2024"
            raw_dir.mkdir(parents=True)
            pl.DataFrame({"trade_date": [days[0]]}).write_parquet(raw_dir / f"SPY_{days[0].isoformat()}.parquet")
            pl.DataFrame({"trade_date": [days[2]]}).write_parquet(raw_dir / f"SPY_{days[2].isoformat()}.parquet")

            fake = FakeDownloader(root)
            updater = IncrementalOptionsUpdater(
                config=DownloadConfig(
                    symbols=["SPY"],
                    start_date=days[0],
                    end_date=days[-1],
                    sync_to_gdrive=False,
                    data_dir=root,
                    consolidate_yearly=False,
                ),
                downloader=fake,  # type: ignore[arg-type]
            )
            # Without forced_since, updater should detect the hole at day 1 and heal it
            failed = updater.run_update(target_end_date=days[-1], forced_since=None)
            self.assertEqual(failed, 0)
            self.assertIn(days[1], fake.seen)

    def test_downloader_skips_days_already_in_processed_yearly_parquet(self) -> None:
        """ThetaOptionsDownloader skips days already consolidated in processed yearly parquet even if raw files are absent."""
        from options_data.downloader import ThetaOptionsDownloader

        day1 = datetime.date(2024, 1, 2)
        day2 = datetime.date(2024, 1, 3)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proc_dir = root / "processed" / "options_eod" / "symbol=SPY"
            proc_dir.mkdir(parents=True)
            # Existing consolidated file with day1
            pl.DataFrame({"trade_date": [day1], "symbol": ["SPY"]}).write_parquet(proc_dir / "SPY_2024.parquet")

            cfg = DownloadConfig(
                symbols=["SPY"],
                start_date=day1,
                end_date=day2,
                data_dir=root,
                skip_existing=True,
                sync_to_gdrive=False,
                consolidate_yearly=False,
                include_underlying_eod=False,
                max_workers=1,
            )
            downloader = ThetaOptionsDownloader(config=cfg, client=mock.MagicMock())
            downloader.get_trading_days = lambda s, e: [day1, day2]  # type: ignore[assignment]
            downloaded: list[datetime.date] = []

            def fake_fetch(sym: str, d: datetime.date) -> pl.DataFrame:
                downloaded.append(d)
                return pl.DataFrame({"trade_date": [d], "symbol": [sym]})

            downloader._fetch_single_day_options = fake_fetch  # type: ignore[assignment]
            downloader.run()

            # day1 was in processed yearly parquet, so only day2 should have been requested!
            self.assertEqual(downloaded, [day2])

    def test_options_viewer_backend_queries(self) -> None:
        """OptionsViewerBackend serves metadata, contract timeseries, and chain smile from Parquet."""
        from options_data.viewer import OptionsViewerBackend

        d1 = datetime.date(2024, 1, 2)
        d2 = datetime.date(2024, 1, 3)
        exp = datetime.date(2024, 2, 16)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proc_dir = root / "processed" / "options_eod" / "symbol=SPY"
            proc_dir.mkdir(parents=True)
            df = pl.DataFrame(
                {
                    "trade_date": [d1, d2, d1, d2],
                    "expiration": [exp, exp, exp, exp],
                    "strike": [500.0, 500.0, 505.0, 505.0],
                    "right": ["call", "call", "put", "put"],
                    "symbol": ["SPY", "SPY", "SPY", "SPY"],
                    "dte": [45, 44, 45, 44],
                    "underlying_price": [502.0, 503.5, 502.0, 503.5],
                    "bid": [10.0, 11.0, 8.0, 7.5],
                    "ask": [10.4, 11.4, 8.4, 7.9],
                    "mid": [10.2, 11.2, 8.2, 7.7],
                    "close": [10.1, 11.1, 8.1, 7.6],
                    "volume": [100, 150, 80, 90],
                    "open_interest": [1000, 1200, 800, 850],
                    "implied_vol": [0.15, 0.16, 0.17, 0.165],
                    "delta": [0.55, 0.58, -0.45, -0.42],
                    "gamma": [0.02, 0.021, 0.02, 0.021],
                    "theta": [-0.12, -0.13, -0.11, -0.12],
                    "vega": [0.40, 0.41, 0.39, 0.40],
                    "vega_per_pct": [0.40, 0.41, 0.39, 0.40],
                    "rho": [0.10, 0.11, -0.09, -0.10],
                    "rho_per_pct": [0.10, 0.11, -0.09, -0.10],
                }
            )
            df.write_parquet(proc_dir / "SPY_2024.parquet")

            viewer = OptionsViewerBackend(
                data_dir=root,
                universe_csv=root / "nonexistent.csv",
                auto_fetch_gdrive=False,
            )
            syms = viewer.list_symbols()
            self.assertEqual(len(syms), 1)
            self.assertEqual(syms[0]["symbol"], "SPY")
            self.assertEqual(syms[0]["cached_years"], [2024])

            meta = viewer.get_symbol_metadata("SPY", year=2024)
            self.assertEqual(meta["expirations"], ["2024-02-16"])
            self.assertEqual(meta["trade_dates"], ["2024-01-02", "2024-01-03"])

            contracts = viewer.get_contracts_for_expiration("SPY", "2024-02-16", year=2024)
            self.assertEqual(contracts["strikes"], [500.0, 505.0])
            self.assertEqual(contracts["atm_strike"], 505.0)

            ts = viewer.get_contract_timeseries("SPY", "2024-02-16", 500.0, "call", year=2024)
            self.assertEqual(ts["count"], 2)
            self.assertEqual(ts["series"][0]["mid"], 10.2)
            self.assertEqual(ts["series"][1]["delta"], 0.58)

            smile = viewer.get_chain_smile("SPY", "2024-01-03", "2024-02-16")
            self.assertEqual(len(smile["chain"]), 2)
            self.assertEqual(smile["underlying_price"], 503.5)

            # Option Chain snapshot query by date
            chain = viewer.get_option_chain("SPY", trade_date="2024-01-03", expiration="2024-02-16")
            self.assertEqual(chain["symbol"], "SPY")
            self.assertEqual(chain["trade_date"], "2024-01-03")
            self.assertEqual(chain["underlying_price"], 503.5)
            self.assertEqual(chain["selected_expiration"], "2024-02-16")
            self.assertEqual(len(chain["strikes"]), 2)
            self.assertEqual(chain["strikes"][0]["strike"], 500.0)
            self.assertIsNotNone(chain["strikes"][0]["call"])
            self.assertIsNone(chain["strikes"][0]["put"])
            self.assertEqual(chain["strikes"][0]["call"]["mid"], 11.2)
            self.assertEqual(chain["strikes"][1]["strike"], 505.0)
            self.assertIsNone(chain["strikes"][1]["call"])
            self.assertIsNotNone(chain["strikes"][1]["put"])
            self.assertEqual(chain["strikes"][1]["put"]["mid"], 7.7)
            self.assertEqual(chain["totals"]["total_volume"], 240)

            # Fallback to latest date if trade_date omitted
            chain_latest = viewer.get_option_chain("SPY")
            self.assertEqual(chain_latest["trade_date"], "2024-01-03")

    def test_viewer_html_chart_layout_separation(self) -> None:
        """Dashboard HTML chart layouts must cleanly separate title and horizontal legend, prevent overflow, and support smooth zoom."""
        from options_data.viewer import INDEX_HTML

        self.assertIn("margin: { l: 65, r: 65, t: 84, b: 45 }", INDEX_HTML)
        self.assertIn("yref: 'container'", INDEX_HTML)
        self.assertIn("yanchor: 'top'", INDEX_HTML)
        self.assertIn("yanchor: 'bottom'", INDEX_HTML)
        self.assertIn(".js-plotly-plot .gtitle", INDEX_HTML)
        self.assertIn("function parsePlotlyDate", INDEX_HTML)
        self.assertIn("function formatPlotlyDate", INDEX_HTML)
        self.assertIn("showZoomHint", INDEX_HTML)
        self.assertIn("chart-zoom-overlay", INDEX_HTML)
        self.assertIn("overflow: hidden", INDEX_HTML)
        self.assertIn("rawDelta = e.deltaX", INDEX_HTML)
        self.assertIn("Daily Option Chain", INDEX_HTML)
        self.assertIn("chain-table", INDEX_HTML)
        self.assertIn("exp-pills-bar", INDEX_HTML)
        self.assertIn("jumpToLifecycle", INDEX_HTML)
        self.assertIn("returnToChain", INDEX_HTML)
        self.assertIn("jumpFromContractToChain", INDEX_HTML)
        self.assertIn("stepTradeDate", INDEX_HTML)
        self.assertIn("filterChainStrikes", INDEX_HTML)

    def test_query_chain_trade_date_and_delta_parameters(self) -> None:
        """query_chain supports trade_date and signed min_delta/max_delta without TypeError."""
        from options_data.loader import OptionsDataLoader

        d1 = datetime.date(2024, 6, 3)
        exp = datetime.date(2024, 7, 19)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            proc_dir = root / "processed" / "options_eod" / "symbol=AAPL"
            proc_dir.mkdir(parents=True)
            df = pl.DataFrame(
                {
                    "trade_date": [d1, d1],
                    "expiration": [exp, exp],
                    "strike": [180.0, 190.0],
                    "right": ["put", "put"],
                    "symbol": ["AAPL", "AAPL"],
                    "dte": [46, 46],
                    "underlying_price": [194.0, 194.0],
                    "bid": [2.50, 4.80],
                    "ask": [2.60, 4.90],
                    "mid": [2.55, 4.85],
                    "close": [2.55, 4.85],
                    "delta": [-0.25, -0.40],
                    "vega_per_pct": [0.15, 0.18],
                    "open_interest_prior_close": [5000, 7500],
                }
            )
            df.write_parquet(proc_dir / "AAPL_2024.parquet")

            loader = OptionsDataLoader(data_dir=root)
            # Call with exact parameters from README example:
            chain = loader.query_chain(
                symbol="AAPL",
                trade_date="2024-06-03",
                right="put",
                min_dte=30,
                max_dte=50,
                min_delta=-0.35,
                max_delta=-0.15,
            )
            self.assertEqual(len(chain), 1)
            self.assertEqual(chain["strike"][0], 180.0)
            self.assertEqual(chain["delta"][0], -0.25)

    def test_empty_day_creates_sentinel_and_is_skipped(self) -> None:
        """An empty day writes a .empty marker and is skipped on subsequent runs."""
        from options_data.downloader import DownloadConfig, ThetaOptionsDownloader

        d = datetime.date(2024, 1, 5)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            cfg = DownloadConfig(
                symbols=["SPY"],
                data_dir=root,
                skip_existing=True,
                sync_to_gdrive=False,
            )
            mock_client = unittest.mock.MagicMock()
            dl = ThetaOptionsDownloader(config=cfg, client=mock_client)
            # Simulate empty DataFrame returned for trade date d
            dl._fetch_single_day_options = unittest.mock.MagicMock(return_value=pl.DataFrame())

            date, rows, status = dl.download_day("SPY", d)
            self.assertEqual(status, "empty")
            self.assertEqual(rows, 0)
            empty_file = dl.daily_parquet_path("SPY", d).with_suffix(".empty")
            self.assertTrue(empty_file.exists())

            # Second call should skip without calling fetch
            dl._fetch_single_day_options.reset_mock()
            date2, rows2, status2 = dl.download_day("SPY", d)
            self.assertEqual(status2, "skipped")
            dl._fetch_single_day_options.assert_not_called()

    def test_updater_recognizes_empty_sentinels_in_hole_scan(self) -> None:
        """updater.py includes both daily and yearly .empty sentinels in covered_dates."""
        from options_data.downloader import DownloadConfig, ThetaOptionsDownloader
        from options_data.updater import IncrementalOptionsUpdater

        d1 = datetime.date(2024, 1, 2)
        d2 = datetime.date(2024, 1, 3)
        d3 = datetime.date(2024, 1, 4)

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw_dir = root / "raw" / "options_eod" / "symbol=SPY" / "year=2024"
            raw_dir.mkdir(parents=True)
            # d1 is parquet, d2 is .empty
            (raw_dir / "SPY_2024-01-02.parquet").write_bytes(b"dummy")
            (raw_dir / "SPY_2024-01-03.empty").touch()

            cfg = DownloadConfig(
                symbols=["SPY"],
                data_dir=root,
                sync_to_gdrive=False,
            )
            mock_client = unittest.mock.MagicMock()
            dl = ThetaOptionsDownloader(config=cfg, client=mock_client)
            dl.get_trading_days = unittest.mock.MagicMock(return_value=[d1, d2, d3])
            dl.download_day = unittest.mock.MagicMock(return_value=(d3, 10, "saved"))
            dl.download_underlying_eod = unittest.mock.MagicMock()

            updater = IncrementalOptionsUpdater(config=cfg, downloader=dl)
            failed = updater.run_update(target_end_date=d3)
            self.assertEqual(failed, 0)

            # d2 was .empty, so updater should NOT request d2; only missing day d3 should be requested!
            requested_days = [call.args[1] for call in dl.download_day.call_args_list]
            self.assertEqual(requested_days, [d3])
            dl.download_underlying_eod.assert_called_with("SPY", start_date=d3)

    def test_empty_sentinel_never_written_for_today(self) -> None:
        """EOD data requested for today before ready must never create durable .empty sentinel."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            today = nyse_today()
            cfg = DownloadConfig(symbols=["SPY"], data_dir=root, sync_to_gdrive=False)
            mock_client = unittest.mock.MagicMock()
            dl = ThetaOptionsDownloader(config=cfg, client=mock_client)
            dl._fetch_single_day_options = unittest.mock.MagicMock(return_value=pl.DataFrame())

            res_date, count, status = dl.download_day("SPY", today)
            self.assertEqual(status, "not_ready")
            empty_marker = dl.daily_parquet_path("SPY", today).with_suffix(".empty")
            self.assertFalse(empty_marker.exists())

    def test_year_filter_excludes_other_years_raw_files(self) -> None:
        """scan_symbol with years=[2024] must strictly exclude raw files from year=2025."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_2024 = root / "raw" / "options_eod" / "symbol=SPY" / "year=2024"
            raw_2025 = root / "raw" / "options_eod" / "symbol=SPY" / "year=2025"
            raw_2024.mkdir(parents=True, exist_ok=True)
            raw_2025.mkdir(parents=True, exist_ok=True)

            df_2024 = pl.DataFrame({
                "symbol": ["SPY"],
                "trade_date": [datetime.date(2024, 6, 3)],
                "expiration": [datetime.date(2024, 6, 21)],
                "strike": [500.0],
                "right": ["call"],
                "bid": [2.0],
                "ask": [2.2],
            })
            df_2025 = pl.DataFrame({
                "symbol": ["SPY"],
                "trade_date": [datetime.date(2025, 1, 6)],
                "expiration": [datetime.date(2025, 1, 17)],
                "strike": [600.0],
                "right": ["call"],
                "bid": [3.0],
                "ask": [3.2],
            })
            df_2024.write_parquet(raw_2024 / "SPY_2024-06-03.parquet")
            df_2025.write_parquet(raw_2025 / "SPY_2025-01-06.parquet")

            loader = OptionsDataLoader(data_dir=root)
            lf_2024 = loader.scan_symbol("SPY", years=[2024])
            res_dates = lf_2024.select(pl.col("trade_date")).collect()["trade_date"].to_list()
            self.assertEqual(res_dates, [datetime.date(2024, 6, 3)])

    def test_loader_rejects_path_traversal_symbols(self) -> None:
        """OptionsDataLoader must reject traversal tickers like '../../etc'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            loader = OptionsDataLoader(data_dir=tmpdir)
            with self.assertRaises(ValueError):
                loader.scan_symbol("../SPY")
            with self.assertRaises(ValueError):
                loader.ensure_local_parquets("SPY/..")

    def test_underlying_catchup_covers_gap_older_than_90_days(self) -> None:
        """download_underlying_eod must start from existing file's latest date if older than start_date."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            und_dir = root / "raw" / "underlying_eod"
            und_dir.mkdir(parents=True, exist_ok=True)
            old_date = datetime.date(2023, 6, 1)
            pl.DataFrame({
                "symbol": ["SPY"],
                "created": ["2023-06-01 16:00:00"],
                "close": [420.0],
            }).write_parquet(und_dir / "SPY_eod.parquet")

            cfg = DownloadConfig(
                symbols=["SPY"],
                data_dir=root,
                start_date=datetime.date(2024, 1, 1),
                end_date=datetime.date(2024, 2, 1),
                sync_to_gdrive=False,
            )
            mock_client = unittest.mock.MagicMock()
            mock_client.stock_history_eod.return_value = pl.DataFrame()
            dl = ThetaOptionsDownloader(config=cfg, client=mock_client)
            dl.download_underlying_eod("SPY", start_date=datetime.date(2024, 1, 1))

            call_args = mock_client.stock_history_eod.call_args
            self.assertEqual(call_args.kwargs["start_date"], old_date)

    def test_one_sided_date_queries_read_all_years(self) -> None:
        """query_chain with only start_date must not restrict scan to start_date.year alone."""
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_2024 = root / "raw" / "options_eod" / "symbol=SPY" / "year=2024"
            raw_2025 = root / "raw" / "options_eod" / "symbol=SPY" / "year=2025"
            raw_2024.mkdir(parents=True, exist_ok=True)
            raw_2025.mkdir(parents=True, exist_ok=True)

            df_2024 = pl.DataFrame({
                "symbol": ["SPY"],
                "trade_date": [datetime.date(2024, 12, 30)],
                "expiration": [datetime.date(2025, 1, 17)],
                "strike": [500.0],
                "right": ["call"],
                "bid": [2.0],
                "ask": [2.2],
            })
            df_2025 = pl.DataFrame({
                "symbol": ["SPY"],
                "trade_date": [datetime.date(2025, 1, 6)],
                "expiration": [datetime.date(2025, 1, 17)],
                "strike": [600.0],
                "right": ["call"],
                "bid": [3.0],
                "ask": [3.2],
            })
            df_2024.write_parquet(raw_2024 / "SPY_2024-12-30.parquet")
            df_2025.write_parquet(raw_2025 / "SPY_2025-01-06.parquet")

            loader = OptionsDataLoader(data_dir=root)
            res = loader.query_chain("SPY", start_date=datetime.date(2024, 12, 1))
            self.assertEqual(res.height, 2)
            self.assertEqual(res["trade_date"].to_list(), [datetime.date(2024, 12, 30), datetime.date(2025, 1, 6)])

    def test_default_configurations_match_production_download_settings(self) -> None:
        """Ensure all default settings in cli.py, updater.py, and downloader.py match production parameters."""
        from options_data.cli import build_parser as build_cli_parser
        from options_data.updater import build_parser as build_updater_parser
        from options_data.downloader import DownloadConfig

        # 1. Check download-options CLI defaults
        cli_args = build_cli_parser().parse_args([])
        self.assertEqual(cli_args.greeks_model, "bs")
        self.assertEqual(cli_args.strike_range, 50)
        self.assertEqual(cli_args.max_dte, 150)
        self.assertEqual(cli_args.universe, "active")
        self.assertEqual(cli_args.rate_type, "sofr")
        self.assertEqual(cli_args.start_date, datetime.date(2016, 1, 1))
        self.assertEqual(cli_args.workers, 4)
        self.assertFalse(cli_args.no_open_interest)
        self.assertFalse(cli_args.no_underlying)

        # 2. Check update-options CLI defaults
        updater_args = build_updater_parser().parse_args([])
        self.assertEqual(updater_args.greeks_model, "bs")
        self.assertEqual(updater_args.strike_range, 50)
        self.assertEqual(updater_args.max_dte, 150)
        self.assertEqual(updater_args.workers, 4)
        self.assertTrue(updater_args.include_open_interest)
        self.assertFalse(updater_args.skip_underlying)

        # 3. Check DownloadConfig class defaults
        cfg = DownloadConfig(symbols=["SPY"])
        self.assertEqual(cfg.greeks_model, "bs")
        self.assertEqual(cfg.strike_range, 50)
        self.assertEqual(cfg.max_dte, 150)
        self.assertEqual(cfg.rate_type, "sofr")
        self.assertEqual(cfg.start_date, datetime.date(2016, 1, 1))
        self.assertTrue(cfg.include_open_interest)
        self.assertTrue(cfg.include_underlying_eod)
        self.assertTrue(cfg.option_use_nbbo)
        self.assertEqual(cfg.max_workers, 4)
        self.assertTrue(cfg.skip_existing)
        self.assertTrue(cfg.consolidate_yearly)
        self.assertTrue(cfg.sync_to_gdrive)


if __name__ == "__main__":
    unittest.main()
