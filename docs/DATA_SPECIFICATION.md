# S&P 500 Options Lakehouse Data Specification

This document defines the dataset specifications, structural invariants, filtering policies, and historical data characteristics for the ThetaData S&P 500 options lakehouse.

---

## 1. DTE (Days-to-Expiration) Policy

- **Standard Policy**: The project-wide default filter is **`max_dte = 150`** (contracts expiring within 150 calendar days from the trade date).
  - This captures weekly, monthly, and quarterly contracts across ~5 expiration months, covering the highest liquidity and trading volume contracts while optimizing storage and transmission overhead.
  - Both CLI tools (`download-options` and `update-options`) and the core `DownloadConfig` enforce `max_dte = 150` by default.
- **Historical Extended Horizons**:
  - Certain historical files (e.g., initial baseline pulls for mega-caps like `AAPL`, `MSFT`, `NVDA`, or specific historical research windows) contain contracts with `dte > 150` days (such as LEAPS expiring up to 2–3 years out).
  - Per data retention policy, existing contracts with `dte > 150` are **deliberately preserved** to retain historical research value without destructive overwrites.
  - All ongoing daily incremental catch-ups and new symbol downloads adhere strictly to the `max_dte = 150` standard rule.

---

## 2. Strike Selection & Pricing

- **Strike Range**: ATM $\pm 50$ strikes (`strike_range = 50`), ensuring complete coverage around the forward/spot price.
- **Quote Convention**: Official OPRA NBBO quotes (`option_use_nbbo = True`).
- **Derived Mid & Spread**:
  - $\text{mid} = \frac{\text{bid} + \text{ask}}{2}$
  - $\text{spread} = \text{ask} - \text{bid}$

---

## 3. Greeks Model & Derived Schema

- **Model**: Black-Scholes (`bs`) using daily SOFR risk-free interest rates.
- **Scaled Sensitivity Measures**:
  - `vega`: Raw mathematical Black-Scholes derivative $\frac{\partial V}{\partial \sigma}$ (100% volatility move).
  - `vega_per_pct`: Standard sensitivity per 1 percentage point move in implied volatility ($\frac{\text{vega}}{100.0}$).
  - `rho`: Raw mathematical derivative $\frac{\partial V}{\partial r}$ (100% interest rate shift).
  - `rho_per_pct`: Standard sensitivity per 1 percentage point shift in rates ($\frac{\text{rho}}{100.0}$).
  - `open_interest_prior_close`: Direct alias of `open_interest`, clarifying that OPRA daily open interest disseminated in the morning reflects the prior session's close.
- **Full Greeks Order Suite**:
  - 1st Order: `delta`, `theta`, `vega`, `vega_per_pct`, `rho`, `rho_per_pct`, `epsilon`, `lambda`.
  - 2nd/Higher Order: `gamma`, `vanna`, `charm`, `vomma`, `veta`, `vera`, `speed`, `zomma`, `color`, `ultima`.

---

## 4. Upstream Data Availability Notes

### 2016 Historical Greeks Availability
- ThetaData's Greeks endpoint (`option_history_greeks_eod`) only provides precomputed Black-Scholes Greeks for major mega-cap equities (e.g. `AAPL`, `MSFT`, `NVDA`, `AMZN`, `GOOGL`, `META`, `SPY`) in Year 2016.
- For non-mega-cap symbols (358 symbols in Year 2016), ThetaData returns `NoDataFoundError` on Greeks endpoints. The pipeline gracefully falls back to `option_history_eod`, preserving complete raw EOD market quotes (bid, ask, close, volume, open interest) across 26 columns.

### Upstream Symbol Exclusion: NVR
- `NVR` (NVR, Inc., trading at ~\$9,000/share) is not listed in ThetaData's options root directory (`client.option_list_symbols()`), as exchange options for this ticker are not carried by ThetaData.

### Exchange Gaps & Corporate Actions (e.g. AIV)
- Apartment Investment and Management Company (`AIV`) experienced an Aimco/AIRC spin-off restructuring in late 2020. Exchange trading sessions on 2019-02-21, 2020-11-03, 2020-12-01, and 2020-12-15 have zero option records on OPRA/ThetaData, and sessions on 2020-11-04, 2020-11-05, 2020-12-02, and 2020-12-16 reflect zero total open interest directly from the exchange. These are verified upstream market realities and are marked with `.empty` sentinels to prevent redundant re-queries.

---

## 5. Directory & Storage Hierarchy

```text
Google Drive Root / Options Strategy / thetadata_parquet /
├── DATA_SPECIFICATION.md           <-- This specification document
├── sp500_universe.csv              <-- Unified universe registry (599 active + 120 excluded)
├── options_eod/                    <-- 503 Active S&P 500 constituents
│   └── symbol=<SYM>/
│       └── <SYM>_<YYYY>.parquet    <-- Yearly consolidated options data
├── options_eod/Removed/            <-- 96 S&P 500 removed but actively trading stocks
│   └── symbol=<SYM>/
│       └── <SYM>_<YYYY>.parquet
├── options_eod/Delisted/           <-- Delisted components (KVUE, LEG, QRVO)
│   └── symbol=<SYM>/
│       └── <SYM>_<YYYY>.parquet
└── underlying_eod/                 <-- Daily stock EOD underlying bars
    └── <SYM>_eod.parquet
```
