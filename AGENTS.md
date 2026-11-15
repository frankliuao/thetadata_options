# Agent Development Guidelines & Repository Architecture

This document defines mandatory engineering guidelines, architectural invariants, and synchronization rules for AI agents and human contributors working on this repository.

---

## 🚨 The Dual-Surface Synchronization Rule (MANDATORY)

The interactive analytics dashboard of this project exists across **two synchronized presentation surfaces**:

1. **The Production Application (`options-viewer`)**:
   - **Entrypoint**: `src/options_data/viewer.py` (run via `poetry run options-viewer` or `python scripts/run_viewer.py`).
   - **Backend Engine**: Local Python HTTP server (`ThreadingHTTPServer`) with lazy Polars DataFrame queries against 51 GB partitioned Apache Parquet lakehouses (`data/processed/options_eod/`) and optional Google Drive synchronization.
   - **Target Audience**: Quant researchers and traders running live analysis locally with real ThetaData subscriptions.

2. **The Static Showcase Demo (`docs/`)**:
   - **Entrypoint**: `docs/index.html` with static mock data in `docs/sample_data/`.
   - **Backend Engine**: In-browser client-side mock query engine reading Black-Scholes synthetic datasets (0 KB backend dependency, 100% compliant with OPRA/ThetaData redistribution policies).
   - **Hosting**: Deployed directly via GitHub Pages (`https://frankliuao.github.io/thetadata_options/`).
   - **Target Audience**: Public portfolio visitors, open-source users evaluating UI/pipeline features without local setup.

### ⚠️ Strict Invariant for AI Agents & Developers:
> **Whenever you add, modify, or fix a UI feature, control, chart, styling, or interaction in the application (`src/options_data/viewer.py`), you MUST also synchronize the static page in `docs/`!**
>
> ❌ **NEVER only update the static page (`docs/index.html`)** while leaving `src/options_data/viewer.py` outdated.
> ❌ **NEVER modify the app (`src/options_data/viewer.py`)** without regenerating `docs/index.html`.
>
> ✅ **ALWAYS maintain 100% feature and visual parity** between the app and the static demo.

---

## 🔄 Standard Workflow for Feature Changes

When developing or improving dashboard features:

1. **Implement in the Core App First**:
   - Edit `src/options_data/viewer.py` (HTML structure, CSS, event handlers, TradingView interactions, Plotly charts, backend endpoints).
2. **Regenerate Static Demo**:
   - Run the automated generator script:
     ```bash
     python scripts/generate_docs_demo.py
     ```
   - This script automatically extracts `INDEX_HTML` from `src/options_data/viewer.py`, injects the client-side router and compliance disclaimers, exports `docs/index.html`, and verifies mock datasets under `docs/sample_data/`.
3. **Verify Both Environments**:
   - **Verify Unit Tests**:
     ```bash
     python -m unittest discover -s tests -v
     ```
     Ensure `tests/test_docs_demo.py` and `tests/test_pipeline.py` pass cleanly (all 35+ tests).
   - **Verify Local Production App**:
     ```bash
     python scripts/run_viewer.py --port 8050
     ```
   - **Verify Static Demo**:
     ```bash
     python -m http.server -d docs 8080
     ```
     Open `http://localhost:8080` to verify that the static demo reflects the new feature identically.

---

## 🏛️ Repository Key Directory Layout

```text
├── src/options_data/
│   ├── viewer.py             <-- CORE APP: Web visualizer, HTTP handler, embedded INDEX_HTML
│   ├── downloader.py         <-- ThetaData v3 gRPC/REST options chain downloader
│   ├── updater.py            <-- Catch-up incremental lakehouse updater
│   ├── loader.py             <-- Polars parquet lakehouse scanner
│   ├── schema.py             <-- Greeks scaling (vega_per_pct, rho_per_pct) & normalization
│   └── gdrive_sync.py        <-- Mutex-protected Google Drive backup
├── docs/                     <-- GITHUB PAGES: Public static demo
│   ├── index.html            <-- Self-contained static dashboard with in-browser query engine
│   └── sample_data/          <-- Synthetic Black-Scholes mock data (AAPL, MSFT, NVDA)
├── scripts/
│   ├── generate_docs_demo.py <-- Automated synchronization tool from viewer.py to docs/
│   ├── run_viewer.py         <-- CLI launcher for options-viewer
│   └── update_options.py     <-- CLI launcher for updater
├── tests/
│   ├── test_pipeline.py      <-- Downloader, updater, schema, and viewer unit tests
│   └── test_docs_demo.py     <-- Static docs and synthetic dataset validation
└── .github/workflows/
    ├── ci.yml                <-- Automated linting and unit test suite
    └── deploy-pages.yml      <-- Automatic GitHub Pages deployment on push to main
```
