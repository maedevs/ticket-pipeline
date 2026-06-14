# RTK Tickets — Data Analyst Take-Home Exercise

A production-minded pipeline that ingests a local JSON event-pricing feed,
normalises the records into analytical tables, runs data quality checks, and
produces business-facing outputs. Built with Python and DuckDB.

---

## Quick start

```bash
pip install -r requirements.txt

python -m src.ingest --input data/raw --database output/ticket_feed.duckdb
python -m src.run_quality_checks --database output/ticket_feed.duckdb
python -m src.generate_outputs --database output/ticket_feed.duckdb --output sample_outputs/
```

Run tests:

```bash
pytest tests/ -v
```

---

## Project layout

```
src/
  __init__.py
  schema.py              — DuckDB table definitions (idempotent CREATE IF NOT EXISTS)
  ingest.py              — Feed ingestion pipeline
  run_quality_checks.py  — 22 data quality checks written to data_quality_results
  generate_outputs.py    — 15 analytical CSV outputs + pricing review + business memo

tests/
  test_pipeline.py       — 24 pytest tests covering helpers, validation, and integration

sql/                     — (queries are embedded in generate_outputs.py for portability)
data/raw/                — Input JSON files (not committed; place files here)
sample_outputs/          — Pre-generated CSVs and memo from the provided dataset
output/                  — DuckDB database written here (gitignored)
```

---

## How to run the pipeline

### 1. Ingest

```bash
python -m src.ingest --input data/raw --database output/ticket_feed.duckdb
```

Reads every `.json` file in `data/raw/`, validates each file, normalises
records into all dimension and fact tables, and records the run in
`ingestion_runs`. Safe to run multiple times — all writes are idempotent.

Files are processed in batches of 10 and committed per batch, so a partial
run leaves the database in a consistent state.

### 2. Quality checks

```bash
python -m src.run_quality_checks --database output/ticket_feed.duckdb
```

Runs 22 checks (ERRORs, WARNINGs, INFO) and writes results to
`data_quality_results`. Checks cover: feed status, pagination completeness,
key presence and uniqueness, timestamp parseability, price validity, price
change direction/sign consistency, forecast sentinel handling, and staleness.

### 3. Analytical outputs

```bash
python -m src.generate_outputs --database output/ticket_feed.duckdb --output sample_outputs/
```

Produces 15 CSV files, a pricing review ranked list, and a business memo.

### 4. Tests

```bash
pytest tests/ -v
```

24 tests across four classes: `TestHelpers`, `TestValidation`, `TestExtraction`,
`TestIngest`. Integration tests use a temporary in-memory DuckDB instance.

---

## Database / storage

DuckDB (file-based, single binary, no server). Output at `output/ticket_feed.duckdb`.

Chosen for: columnar analytics performance, SQL-native JSON support, zero
infrastructure, and simple Python bindings.

---

## Schema design

### Why split event from market state?

`events` holds stable descriptive attributes (title, category, datetime, timezone)
that are unlikely to change between feed deliveries. `event_market_state` holds
volatile market data (price, quantity, forecast, freshness) that changes on every
pull. Keeping them separate means:

- Historic price and supply snapshots can accumulate over time (one row per run
  per event in `event_market_state`)
- Joins for reporting are clean — event identity is stable even when market data
  refreshes
- The `events` table is small and cache-friendly for repeated categorical lookups

### Why `performer_slug` and `venue_slug` as natural keys?

The feed provides these slugs consistently and they are human-readable and stable.
A surrogate key would add an indirection layer with no benefit given the data is
feed-sourced and slugs are already unique identifiers in the upstream system.
If the feed ever reused a slug for a different entity this assumption would need
revisiting, but that is not observed in the current data.

### Tables

| Table | Purpose |
|---|---|
| `ingestion_runs` | One row per pipeline invocation; tracks files found/loaded and overall status |
| `raw_feed_files` | One row per JSON file; captures pagination metadata and date filters |
| `raw_event_records` | Full raw JSON per event for audit trail and debugging |
| `events` | Stable event attributes (id, title, category, datetimes, timezone) |
| `performers` | Performer dimension; natural key = `performer_slug` |
| `venues` | Venue dimension; natural key = `venue_slug` |
| `event_performers` | Event ↔ performer bridge (many-to-many capable) |
| `event_venues` | Event ↔ venue bridge |
| `event_market_state` | Current price, supply, forecast, freshness per event per run |
| `event_price_change_windows` | Normalised 3/7/14/30-day price movement (long format) |
| `data_quality_results` | Outcomes of every quality check per run |

---

## Data quality checks implemented

22 checks across three severity levels:

**ERROR** — blocks trust in the data if triggered:
- Feed status = success on every file
- Feed files have metadata
- Event ID present
- Event ID unique in events table
- Event datetime parseable
- Event UTC datetime parseable
- `stats_last_updated_at` parseable
- `get_in_price` numeric (not null)
- Ticket quantity non-negative
- Malformed files and events from ingest

**WARNING** — investigate before using in analysis:
- Pagination record count matches loaded count
- Offset sequence has no gaps
- Performer slug present
- Venue slug present
- `event_datetime` and `event_utc_datetime` within 24 hours of each other
- `get_in_price = 0` flagged (not assumed to be free tickets)
- Price change direction matches sign of `raw_change`
- Suspicious price change (>100% in any window)
- Forecast sentinel value (-1 or -100) not treated as a real prediction
- `stats_last_updated_at` stale (>48 hours)
- `mock_data_point` flag present

**INFO** — informational observations:
- `insufficient_data = true` records have zero raw/percent values
- Events with `forecast_is_available = false` that have non-sentinel forecast values

---

## Analytical outputs produced

| File | Description |
|---|---|
| `01_events_by_category.csv` | Event count by `category_type` and `event_category_name` |
| `02_events_by_geography.csv` | Event count by country, state, city |
| `03_top20_by_get_in_price.csv` | Top 20 events by get-in price (zero-price excluded) |
| `04_top20_by_ticket_quantity.csv` | Top 20 events by available ticket quantity |
| `05_top20_3day_price_movement.csv` | Top 20 by absolute 3-day price movement |
| `06_top20_7day_price_movement.csv` | Top 20 by absolute 7-day price movement |
| `07_zero_price_events.csv` | All events with `get_in_price = 0` (flagged for review) |
| `08_stale_stats.csv` | Events with `stats_last_updated_at` > 48 hours old |
| `09_forecast_available_events.csv` | Events with real (non-sentinel) forecast values |
| `10_forecast_coverage_by_category.csv` | Forecast availability rate by category |
| `11_median_price_by_category.csv` | Median and mean get-in price by category |
| `12_median_price_by_city_venue.csv` | Median get-in price by city and venue |
| `13_suspicious_price_changes.csv` | Events with >100% price movement in any window |
| `14_event_counts_by_source_file.csv` | Loaded vs expected count per file/offset |
| `15_dq_failure_counts.csv` | All quality check results with affected counts |
| `pricing_review_ranked.csv` | Top 100 events ranked by rule-based review score (stretch A) |
| `business_memo.txt` | Plain-text summary of key findings and recommendations |

---

## Key findings from the provided dataset

- **14,000 events** across 140 files; offsets 0–13,900 (100 per page, all complete)
- **5,420 performers** and **4,095 venues**
- **THEATER** is the largest category (6,348 events), followed by SPORTS and CONCERT
- **Las Vegas, NV** has the most events (1,136)
- Highest get-in price: **$10,581** ("Iceboy - The Musical")
- **2,401 events (17%)** have `get_in_price = 0` — sold-out or missing data, not free
- **251 events** have price changes >100% in at least one window — warrant review
- **Forecast coverage is low at 5.4%** — the model is still being calibrated for most events
- `forecast_value = -100` is a sentinel meaning "coming soon"; never treated as a prediction
- All 14,000 stats are older than 48 hours relative to the run time (the feed
  is a point-in-time snapshot from 2026-05-27; staleness is expected in this context)

---

## Timezone handling

`event_datetime` in the feed is an ISO string formatted as UTC (despite the
field name implying local time). `event_utc_datetime` is the authoritative UTC
stamp. Both are stored as `TIMESTAMPTZ` after normalising the `Z` suffix to
`+00:00`. All cross-event time comparisons use `event_utc_datetime`. The
`event_timezone` string (e.g. `EDT`, `PDT`, `HST`) is preserved for display
but not used for conversion — it is not always an IANA timezone identifier.

---

## Pricing review score (Stretch Option A)

`pricing_review_ranked.csv` ranks events using a weighted rule-based score:

| Component | Weight | Signal |
|---|---|---|
| Price vs category median | 25% | Is this event priced above typical? |
| Low ticket supply | 20% | Are tickets scarce? |
| 3-day price movement magnitude | 25% | Is the price moving fast? |
| Forecast confidence | 15% | Does the model expect further movement? |
| Days until event | 10% | Is this time-sensitive? |
| Stats staleness | 5% | Is the data fresh enough to act on? |

All components are normalised 0–1. The score is not a probability — it is a
prioritisation heuristic for directing manual review attention.

Limitation: built on a single snapshot, so "movement" is only observable from
historical windows embedded in the feed, not from comparing across ingestion runs.

---

## Known limitations

- **Single snapshot**: the pipeline is designed to accumulate `event_market_state`
  rows across runs, but true time-series analysis requires multiple feed deliveries.
- **Staleness is relative**: the 48-hour threshold is configurable but arbitrary.
  In a production context this would be calibrated to the feed's refresh SLA.
- **Forecast model coverage**: only 5.4% of events have real forecast values.
  The model appears to be selectively deployed, not a gap in the pipeline.
- **Zero-price events**: 17% of events have `get_in_price = 0`. These are flagged
  but not dropped — downstream consumers should filter or investigate them.
- **`event_timezone` is not always an IANA key**: values like `EDT` and `PDT` are
  abbreviations, not database-resolvable timezone strings. Stored as-is.
- **Raw JSON storage**: `raw_event_records` preserves full JSON per event for
  auditability but is the largest table. Could be moved to object storage in production.

---

## What I would improve with more time

- **Incremental ingestion**: track which files have been loaded by content hash, skip
  unchanged files on re-runs without re-processing.
- **Streaming inserts**: replace batch executemany with DuckDB's `INSERT INTO ... SELECT`
  from an in-memory relation for faster writes on large datasets.
- **dbt or SQLMesh**: replace embedded SQL strings with a proper transformation layer
  that supports lineage, testing, and documentation.
- **Scheduling and monitoring**: wrap the three commands in an Airflow or Prefect DAG
  with alerting on quality check failures.
- **CI/CD**: add GitHub Actions to run `pytest` on every PR; add a smoke test that
  runs the full pipeline against a small fixture file.
- **Expanded quality checks**: cross-run price drift detection, venue/performer name
  drift detection (same slug, different name), event deduplication across overlapping
  offset pages.
- **Forecast model** (Stretch B/C): with multiple snapshots, a gradient-boosted
  classifier predicting 3-day direction using get-in price, ticket quantity, category,
  days until event, and venue history would be feasible. Leakage risk: must exclude all
  price-change window fields when predicting any price-change direction.
