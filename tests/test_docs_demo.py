"""Tests for static GitHub Pages documentation and synthetic mock data demo."""

import json
from pathlib import Path
import unittest

ROOT_DIR = Path(__file__).resolve().parent.parent
DOCS_DIR = ROOT_DIR / "docs"
SAMPLE_DATA_DIR = DOCS_DIR / "sample_data"


class DocsDemoTests(unittest.TestCase):
    def test_docs_directory_and_index_html_exist(self) -> None:
        index_file = DOCS_DIR / "index.html"
        self.assertTrue(index_file.exists(), "docs/index.html must exist")
        html = index_file.read_text(encoding="utf-8")
        self.assertIn("SYNTHETIC MOCK DATA ONLY", html)
        self.assertIn("DEMO MODE", html)
        self.assertIn("Plotly", html)
        self.assertIn("mockApiGet", html)
        self.assertIn('type="date"', html)

    def test_symbols_json_registry(self) -> None:
        symbols_file = SAMPLE_DATA_DIR / "symbols.json"
        self.assertTrue(symbols_file.exists(), "docs/sample_data/symbols.json must exist")
        with open(symbols_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        self.assertIn("symbols", data)
        sym_names = {s["symbol"] for s in data["symbols"]}
        self.assertSetEqual(sym_names, {"AAPL", "MSFT", "NVDA"})
        for item in data["symbols"]:
            self.assertTrue(item.get("is_mock"), f"{item['symbol']} should be flagged as mock")

    def test_symbol_sample_data_content_and_greeks(self) -> None:
        required_keys = {
            "trade_date",
            "symbol",
            "expiration",
            "dte",
            "strike",
            "right",
            "underlying_price",
            "bid",
            "ask",
            "mid",
            "volume",
            "open_interest",
            "implied_vol",
            "delta",
            "gamma",
            "theta",
            "vega",
            "vega_per_pct",
        }
        for sym in ("AAPL", "MSFT", "NVDA"):
            file_path = SAMPLE_DATA_DIR / f"{sym}.json"
            self.assertTrue(file_path.exists(), f"{file_path} must exist")
            with open(file_path, "r", encoding="utf-8") as f:
                records = json.load(f)
            self.assertGreater(len(records), 100, f"{sym} should have >= 100 mock records")
            for rec in records:
                self.assertTrue(required_keys.issubset(rec.keys()), f"Missing keys in {sym} record: {required_keys - set(rec.keys())}")
                self.assertGreater(rec["underlying_price"], 0)
                self.assertGreater(rec["strike"], 0)
                self.assertIn(rec["right"], ("call", "put"))
                self.assertGreaterEqual(rec["dte"], 0)


if __name__ == "__main__":
    unittest.main()
