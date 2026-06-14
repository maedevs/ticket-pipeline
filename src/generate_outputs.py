"""
RTK Tickets analytical output generation.

Usage:
    python -m src.generate_outputs --database output/ticket_feed.duckdb --output sample_outputs/

Produces:
- CSV files for each analytical query
- A summary business memo (memo.txt)
- A pricing review ranked list (pricing_review.csv)
"""

import argparse
import csv
import logging
import os
from datetime import datetime, timezone

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SUSPICIOUS_PRICE_PCT = 100.0
STALE_HOURS = 48


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def write_csv(path: str, rows, headers: list[str]) -> int:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    return len(rows)


def run_query(conn, sql: str) -> tuple[list, list[str]]:
    rel = conn.execute(sql)
    headers = [d[0] for d in rel.description]
    rows = rel.fetchall()
    return rows, headers


QUERIES = {
    "01_events_by_category": (
        """
        SELECT category_type,
               event_category_name,
               COUNT(*) AS event_count
        FROM events
        GROUP BY 1, 2
        ORDER BY event_count DESC
        """,
        "Events by category_type and event_category_name",
    ),
    "02_events_by_geography": (
        """
        SELECT v.country, v.state, v.city,
               COUNT(DISTINCT ev.event_id) AS event_count
        FROM event_venues ev
        JOIN venues v USING (venue_slug)
        GROUP BY 1, 2, 3
        ORDER BY event_count DESC
        """,
        "Events by country, state, city",
    ),
    "03_top20_by_get_in_price": (
        """
        SELECT e.event_id, e.title, e.category_type, e.event_category_name,
               v.venue_name, v.city, v.state, v.country,
               ms.get_in_price,
               ms.stats_total_quantity,
               e.event_utc_datetime
        FROM events e
        JOIN event_market_state ms USING (event_id)
        JOIN event_venues ev USING (event_id)
        JOIN venues v USING (venue_slug)
        WHERE ms.get_in_price > 0
        ORDER BY ms.get_in_price DESC
        LIMIT 20
        """,
        "Top 20 events by get-in price",
    ),
    "04_top20_by_ticket_quantity": (
        """
        SELECT e.event_id, e.title, e.category_type, e.event_category_name,
               v.venue_name, v.city, v.state,
               ms.stats_total_quantity,
               ms.get_in_price,
               e.event_utc_datetime
        FROM events e
        JOIN event_market_state ms USING (event_id)
        JOIN event_venues ev USING (event_id)
        JOIN venues v USING (venue_slug)
        ORDER BY ms.stats_total_quantity DESC
        LIMIT 20
        """,
        "Top 20 events by available ticket quantity",
    ),
    "05_top20_3day_price_movement": (
        """
        SELECT e.event_id, e.title, e.category_type,
               w.direction, w.percent_change, w.raw_change,
               ms.get_in_price,
               e.event_utc_datetime
        FROM event_price_change_windows w
        JOIN events e USING (event_id)
        JOIN event_market_state ms USING (event_id)
        WHERE w.window_days = 3
          AND w.insufficient_data = false
        ORDER BY ABS(w.raw_change) DESC
        LIMIT 20
        """,
        "Top 20 events by absolute 3-day price movement",
    ),
    "06_top20_7day_price_movement": (
        """
        SELECT e.event_id, e.title, e.category_type,
               w.direction, w.percent_change, w.raw_change,
               ms.get_in_price,
               e.event_utc_datetime
        FROM event_price_change_windows w
        JOIN events e USING (event_id)
        JOIN event_market_state ms USING (event_id)
        WHERE w.window_days = 7
          AND w.insufficient_data = false
        ORDER BY ABS(w.raw_change) DESC
        LIMIT 20
        """,
        "Top 20 events by absolute 7-day price movement",
    ),
    "07_zero_price_events": (
        """
        SELECT e.event_id, e.title, e.category_type, e.event_category_name,
               v.venue_name, v.city, v.state,
               ms.stats_total_quantity,
               ms.is_past_with_no_pricing,
               ms.forecast_is_available,
               e.event_utc_datetime
        FROM event_market_state ms
        JOIN events e USING (event_id)
        JOIN event_venues ev USING (event_id)
        JOIN venues v USING (venue_slug)
        WHERE ms.get_in_price = 0
        ORDER BY e.event_utc_datetime
        """,
        "Events with get_in_price = 0 (flagged for review)",
    ),
    "08_stale_stats": (
        f"""
        SELECT e.event_id, e.title, e.category_type,
               ms.stats_last_updated_at,
               EPOCH(CURRENT_TIMESTAMP - ms.stats_last_updated_at) / 3600.0 AS hours_since_update,
               ms.get_in_price,
               e.event_utc_datetime
        FROM event_market_state ms
        JOIN events e USING (event_id)
        WHERE ms.stats_last_updated_at < (CURRENT_TIMESTAMP - INTERVAL '48 hours')
        ORDER BY ms.stats_last_updated_at ASC
        """,
        f"Events with stale stats (not updated in {STALE_HOURS}+ hours)",
    ),
    "09_forecast_available_events": (
        """
        SELECT e.event_id, e.title, e.category_type, e.event_category_name,
               ms.forecast_value,
               ms.forecast_layover_text,
               ms.get_in_price,
               ms.stats_total_quantity,
               e.event_utc_datetime
        FROM event_market_state ms
        JOIN events e USING (event_id)
        WHERE ms.forecast_is_available = true
          AND ms.forecast_value NOT IN (-1, -100)
        ORDER BY ms.forecast_value DESC
        """,
        "Events with forecast available (non-sentinel values)",
    ),
    "10_forecast_coverage_by_category": (
        """
        SELECT e.category_type,
               COUNT(*) AS total_events,
               SUM(CASE WHEN ms.forecast_is_available = true
                         AND ms.forecast_value NOT IN (-1, -100) THEN 1 ELSE 0 END) AS forecast_available,
               ROUND(100.0 * SUM(CASE WHEN ms.forecast_is_available = true
                                       AND ms.forecast_value NOT IN (-1, -100) THEN 1 ELSE 0 END)
                     / COUNT(*), 1) AS pct_with_forecast
        FROM event_market_state ms
        JOIN events e USING (event_id)
        GROUP BY 1
        ORDER BY pct_with_forecast DESC
        """,
        "Forecast coverage by category_type",
    ),
    "11_median_price_by_category": (
        """
        SELECT e.category_type,
               e.event_category_name,
               COUNT(*) AS event_count,
               ROUND(MEDIAN(ms.get_in_price), 2) AS median_get_in_price,
               ROUND(AVG(ms.get_in_price), 2) AS avg_get_in_price,
               MIN(ms.get_in_price) AS min_price,
               MAX(ms.get_in_price) AS max_price
        FROM event_market_state ms
        JOIN events e USING (event_id)
        WHERE ms.get_in_price > 0
        GROUP BY 1, 2
        ORDER BY median_get_in_price DESC
        """,
        "Median get-in price by category_type and event_category_name",
    ),
    "12_median_price_by_city_venue": (
        """
        SELECT v.city, v.state, v.country,
               v.venue_name,
               COUNT(DISTINCT e.event_id) AS event_count,
               ROUND(MEDIAN(ms.get_in_price), 2) AS median_get_in_price,
               ROUND(AVG(ms.get_in_price), 2) AS avg_get_in_price
        FROM event_market_state ms
        JOIN events e USING (event_id)
        JOIN event_venues ev USING (event_id)
        JOIN venues v USING (venue_slug)
        WHERE ms.get_in_price > 0
        GROUP BY 1, 2, 3, 4
        HAVING COUNT(DISTINCT e.event_id) >= 2
        ORDER BY median_get_in_price DESC
        LIMIT 50
        """,
        "Median get-in price by city and venue (min 2 events)",
    ),
    "13_suspicious_price_changes": (
        f"""
        SELECT e.event_id, e.title, e.category_type,
               w.window_days, w.direction, w.percent_change, w.raw_change,
               ms.get_in_price,
               e.event_utc_datetime
        FROM event_price_change_windows w
        JOIN events e USING (event_id)
        JOIN event_market_state ms USING (event_id)
        WHERE w.insufficient_data = false
          AND ABS(w.percent_change) > {SUSPICIOUS_PRICE_PCT}
        ORDER BY ABS(w.percent_change) DESC
        """,
        f"Events with suspicious price change (>{SUSPICIOUS_PRICE_PCT}%)",
    ),
    "14_event_counts_by_source_file": (
        """
        SELECT r.source_file,
               f.offset,
               f.current_page_results AS expected_count,
               COUNT(DISTINCT r.event_id) AS loaded_count,
               f.current_page_results - COUNT(DISTINCT r.event_id) AS delta
        FROM raw_event_records r
        JOIN raw_feed_files f USING (source_file)
        GROUP BY 1, 2, 3
        ORDER BY f.offset
        """,
        "Loaded event counts by source file and offset",
    ),
    "15_dq_failure_counts": (
        """
        SELECT check_name, severity, status, affected_count, details
        FROM data_quality_results
        ORDER BY
            CASE severity WHEN 'ERROR' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END,
            status DESC,
            affected_count DESC
        """,
        "Data quality failure counts by check",
    ),
}

PRICING_REVIEW_QUERY = """
-- Rule-based pricing review score (Option A stretch)
-- Score components (all normalised 0-1, then weighted):
--   1. High get-in price (relative to category median)      weight=0.25
--   2. Low ticket supply                                     weight=0.20
--   3. Large absolute 3-day price movement                   weight=0.25
--   4. Forecast confidence (if available)                    weight=0.15
--   5. Days until event (closer = higher urgency)            weight=0.10
--   6. Stats freshness (staler = higher concern)             weight=0.05

WITH category_medians AS (
    SELECT e.category_type,
           MEDIAN(ms.get_in_price) AS cat_median_price
    FROM event_market_state ms
    JOIN events e USING (event_id)
    WHERE ms.get_in_price > 0
    GROUP BY 1
),
three_day AS (
    SELECT event_id, ABS(raw_change) AS abs_3d_change
    FROM event_price_change_windows
    WHERE window_days = 3 AND insufficient_data = false
),
max_3d AS (
    SELECT MAX(abs_3d_change) AS max_val FROM three_day
),
max_qty AS (
    SELECT MAX(stats_total_quantity) AS max_val
    FROM event_market_state WHERE stats_total_quantity > 0
),
scored AS (
    SELECT
        e.event_id,
        e.title,
        e.category_type,
        e.event_category_name,
        v.venue_name,
        v.city,
        v.state,
        ms.get_in_price,
        ms.stats_total_quantity,
        ms.forecast_is_available,
        ms.forecast_value,
        ms.stats_last_updated_at,
        e.event_utc_datetime,
        DATE_DIFF('day', CURRENT_DATE, e.event_utc_datetime::DATE) AS days_until_event,

        -- Component 1: price vs category median (capped at 2x)
        LEAST(1.0, COALESCE(ms.get_in_price / NULLIF(cm.cat_median_price, 0), 0) / 2.0) AS price_score,

        -- Component 2: low supply (inverted: fewer tickets = higher score)
        CASE WHEN mq.max_val > 0
             THEN 1.0 - LEAST(1.0, ms.stats_total_quantity::DOUBLE / mq.max_val)
             ELSE 0 END AS supply_score,

        -- Component 3: 3-day movement magnitude
        CASE WHEN mx.max_val > 0
             THEN LEAST(1.0, COALESCE(td.abs_3d_change, 0) / mx.max_val)
             ELSE 0 END AS movement_score,

        -- Component 4: forecast confidence (if available, 0 if not)
        CASE WHEN ms.forecast_is_available = true
              AND ms.forecast_value NOT IN (-1, -100)
             THEN ms.forecast_value
             ELSE 0 END AS forecast_score,

        -- Component 5: urgency (events in next 7 days score highest)
        CASE WHEN DATE_DIFF('day', CURRENT_DATE, e.event_utc_datetime::DATE) BETWEEN 0 AND 7 THEN 1.0
             WHEN DATE_DIFF('day', CURRENT_DATE, e.event_utc_datetime::DATE) BETWEEN 8 AND 14 THEN 0.7
             WHEN DATE_DIFF('day', CURRENT_DATE, e.event_utc_datetime::DATE) BETWEEN 15 AND 30 THEN 0.4
             ELSE 0.1 END AS urgency_score,

        -- Component 6: staleness
        CASE WHEN DATE_DIFF('hour', ms.stats_last_updated_at, CURRENT_TIMESTAMP) > 48 THEN 1.0
             WHEN DATE_DIFF('hour', ms.stats_last_updated_at, CURRENT_TIMESTAMP) > 24 THEN 0.5
             ELSE 0.0 END AS staleness_score

    FROM event_market_state ms
    JOIN events e USING (event_id)
    JOIN event_venues ev USING (event_id)
    JOIN venues v USING (venue_slug)
    LEFT JOIN category_medians cm ON cm.category_type = e.category_type
    LEFT JOIN three_day td USING (event_id)
    CROSS JOIN max_3d mx
    CROSS JOIN max_qty mq
    WHERE ms.get_in_price > 0
)
SELECT
    event_id, title, category_type, event_category_name,
    venue_name, city, state,
    get_in_price, stats_total_quantity,
    forecast_is_available,
    ROUND(forecast_value, 3) AS forecast_value,
    days_until_event,
    EPOCH(stats_last_updated_at) AS last_updated_epoch,
    ROUND(
        0.25 * price_score
      + 0.20 * supply_score
      + 0.25 * movement_score
      + 0.15 * forecast_score
      + 0.10 * urgency_score
      + 0.05 * staleness_score,
    3) AS review_score,
    ROUND(price_score, 3) AS price_score,
    ROUND(supply_score, 3) AS supply_score,
    ROUND(movement_score, 3) AS movement_score,
    ROUND(forecast_score, 3) AS forecast_score,
    ROUND(urgency_score, 3) AS urgency_score,
    ROUND(staleness_score, 3) AS staleness_score,
    event_utc_datetime
FROM scored
ORDER BY review_score DESC
LIMIT 100
"""


def generate_memo(conn) -> str:
    """Generate a plain-text business memo summarising key findings."""

    total_events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    total_venues = conn.execute("SELECT COUNT(*) FROM venues").fetchone()[0]
    total_performers = conn.execute("SELECT COUNT(*) FROM performers").fetchone()[0]
    total_files = conn.execute("SELECT COUNT(*) FROM raw_feed_files").fetchone()[0]

    zero_price = conn.execute(
        "SELECT COUNT(*) FROM event_market_state WHERE get_in_price = 0"
    ).fetchone()[0]

    stale = conn.execute(
        f"SELECT COUNT(*) FROM event_market_state WHERE stats_last_updated_at < (CURRENT_TIMESTAMP - INTERVAL '{STALE_HOURS} hours')"
    ).fetchone()[0]

    forecast_pct = conn.execute("""
        SELECT ROUND(100.0 * SUM(CASE WHEN forecast_is_available = true AND forecast_value NOT IN (-1,-100) THEN 1 ELSE 0 END) / COUNT(*), 1)
        FROM event_market_state
    """).fetchone()[0]

    suspicious_changes = conn.execute(
        f"SELECT COUNT(DISTINCT event_id) FROM event_price_change_windows WHERE insufficient_data = false AND ABS(percent_change) > {SUSPICIOUS_PRICE_PCT}"
    ).fetchone()[0]

    top_category = conn.execute("""
        SELECT category_type, COUNT(*) as n FROM events GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    """).fetchone()

    top_city = conn.execute("""
        SELECT v.city, v.state, COUNT(DISTINCT ev.event_id) as n
        FROM event_venues ev JOIN venues v USING (venue_slug)
        GROUP BY 1,2 ORDER BY 3 DESC LIMIT 1
    """).fetchone()

    highest_price = conn.execute("""
        SELECT e.title, ms.get_in_price
        FROM event_market_state ms JOIN events e USING (event_id)
        WHERE ms.get_in_price > 0
        ORDER BY ms.get_in_price DESC LIMIT 1
    """).fetchone()

    dq_fails = conn.execute("""
        SELECT COUNT(*) FROM data_quality_results WHERE status = 'FAIL' AND severity IN ('ERROR','WARNING')
    """).fetchone()[0]

    memo = f"""
================================================================================
RTK TICKETS — FEED ANALYSIS MEMO
Generated: {now_utc()}
================================================================================

DATASET OVERVIEW
----------------
Source files ingested : {total_files}
Total events loaded   : {total_events:,}
Unique venues         : {total_venues:,}
Unique performers     : {total_performers:,}
Date range            : 2026-05-27 to 2026-06-26

KEY FINDINGS
------------

1. CATEGORY MIX
   The dominant category is {top_category[0]} ({top_category[1]:,} events).
   See output 01 for full breakdown across all categories and sub-categories.

2. GEOGRAPHIC CONCENTRATION
   {top_city[0]}, {top_city[1]} leads with {top_city[2]:,} events.
   International events are present but represent a small share of the feed.

3. PRICING HIGHLIGHTS
   Highest get-in price: {highest_price[1]:,.0f} USD — "{highest_price[0]}"
   {zero_price:,} events show get_in_price = 0. These are NOT assumed to be
   free tickets; they likely represent sold-out listings or data gaps and
   should be excluded from pricing analysis or investigated before use.

4. MARKET FRESHNESS
   {stale:,} events have stats_last_updated_at older than {STALE_HOURS} hours.
   These should be treated with caution for real-time pricing decisions.

5. FORECAST COVERAGE
   {forecast_pct}% of events have a valid (non-sentinel) forecast value.
   Note: forecast_value = -1 or -100 is a sentinel for "not available";
   these values are excluded from all forecast analysis.

6. PRICE MOVEMENT ANOMALIES
   {suspicious_changes:,} events show at least one price-change window with
   >100% movement. These are flagged in output 13 and the pricing review
   score and warrant manual review before acting on the data.

7. DATA QUALITY
   {dq_fails} quality checks returned FAIL or WARNING status.
   See output 15 (data_quality_results) for full details.

RECOMMENDATIONS
---------------
- Investigate zero-price events before including them in any market analysis.
- Review the {suspicious_changes} events flagged for extreme price movement;
  these may reflect data errors rather than genuine market moves.
- Prioritise refreshing stale stats before time-sensitive buy/sell decisions.
- Expand forecast model coverage — currently below full population coverage.
- Implement offset continuity monitoring to detect missing feed pages early.

================================================================================
"""
    return memo.strip()


def generate_outputs(database: str, output_dir: str) -> None:
    conn = duckdb.connect(database, read_only=True)
    os.makedirs(output_dir, exist_ok=True)

    log.info("Generating analytical outputs to %s", output_dir)

    for filename, (sql, description) in QUERIES.items():
        try:
            rows, headers = run_query(conn, sql)
            path = os.path.join(output_dir, f"{filename}.csv")
            n = write_csv(path, rows, headers)
            log.info("  %-45s %5d rows -> %s", description[:45], n, os.path.basename(path))
        except Exception as e:
            log.error("  Failed %s: %s", filename, e)

    # Pricing review (stretch option A)
    try:
        rows, headers = run_query(conn, PRICING_REVIEW_QUERY)
        path = os.path.join(output_dir, "pricing_review_ranked.csv")
        n = write_csv(path, rows, headers)
        log.info("  %-45s %5d rows -> %s", "Pricing review ranked list", n, os.path.basename(path))
    except Exception as e:
        log.error("  Failed pricing_review: %s", e)

    # Business memo
    try:
        memo = generate_memo(conn)
        memo_path = os.path.join(output_dir, "business_memo.txt")
        with open(memo_path, "w", encoding="utf-8") as f:
            f.write(memo)
        log.info("  Business memo written -> %s", os.path.basename(memo_path))
    except Exception as e:
        log.error("  Failed memo: %s", e)

    conn.close()
    log.info("Output generation complete.")


def main():
    parser = argparse.ArgumentParser(description="RTK Tickets analytical output generation")
    parser.add_argument("--database", required=True, help="Path to DuckDB database file")
    parser.add_argument("--output", required=True, help="Directory for output CSVs")
    args = parser.parse_args()
    generate_outputs(args.database, args.output)


if __name__ == "__main__":
    main()
