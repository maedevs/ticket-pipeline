"""
RTK Tickets data quality checks.

Usage:
    python -m src.run_quality_checks --database output/ticket_feed.duckdb

Each check writes a row to data_quality_results. Re-running is idempotent
(ON CONFLICT DO UPDATE so results reflect the latest run).
"""

import argparse
import logging
import uuid
from datetime import datetime, timezone

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SENTINEL_FORECAST_VALUES = (-1, -100)
SUSPICIOUS_PRICE_PCT = 100.0
STALE_HOURS = 48


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def get_latest_run_id(conn) -> str | None:
    row = conn.execute(
        "SELECT run_id FROM ingestion_runs WHERE status = 'success' ORDER BY finished_at DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


class QualityChecker:
    def __init__(self, conn, run_id: str):
        self.conn = conn
        self.run_id = run_id
        self.results = []

    def check(self, name: str, severity: str, query: str, threshold: int = 0, details_query: str = None):
        """Run a count query; PASS if count <= threshold, else FAIL."""
        ts = now_utc()
        try:
            count = self.conn.execute(query).fetchone()[0]
            status = "PASS" if count <= threshold else "FAIL"
            details = None
            if status == "FAIL" and details_query:
                rows = self.conn.execute(details_query).fetchall()
                details = "; ".join(str(r) for r in rows[:10])
        except Exception as e:
            count = -1
            status = "FAIL"
            details = f"Check error: {e}"

        check_id = f"{self.run_id}:{name}"
        self.conn.execute("""
            INSERT INTO data_quality_results
                (check_id, run_id, check_name, severity, status, affected_count, details, checked_at)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT (check_id) DO UPDATE SET
                status = excluded.status,
                affected_count = excluded.affected_count,
                details = excluded.details,
                checked_at = excluded.checked_at
        """, [check_id, self.run_id, name, severity, status, count, details, ts])

        icon = "✓" if status == "PASS" else "✗"
        log.info("  [%s] %s %s (affected: %d)", severity, icon, name, count)
        self.results.append((name, severity, status, count))
        return count, status

    def summary(self):
        fails = [(n, sev, cnt) for n, sev, st, cnt in self.results if st == "FAIL"]
        log.info("--- Quality check summary: %d checks, %d failures ---", len(self.results), len(fails))
        for n, sev, cnt in fails:
            log.warning("  FAIL [%s] %s (%d affected)", sev, n, cnt)


def run_checks(database: str) -> None:
    conn = duckdb.connect(database)
    run_id = get_latest_run_id(conn)
    if not run_id:
        log.error("No completed ingestion run found. Run ingest first.")
        return

    log.info("Running quality checks against run %s", run_id)
    qc = QualityChecker(conn, run_id)

    # ── Feed-level checks ──────────────────────────────────────────────────

    qc.check(
        "feed_status_success",
        "ERROR",
        "SELECT COUNT(*) FROM raw_feed_files WHERE feed_status != 'success'",
        details_query="SELECT source_file, feed_status FROM raw_feed_files WHERE feed_status != 'success' LIMIT 5",
    )

    qc.check(
        "feed_files_have_metadata",
        "ERROR",
        "SELECT COUNT(*) FROM raw_feed_files WHERE total_db_matches IS NULL",
        details_query="SELECT source_file FROM raw_feed_files WHERE total_db_matches IS NULL LIMIT 5",
    )

    qc.check(
        "pagination_record_count_matches",
        "WARNING",
        """
        SELECT COUNT(*) FROM (
            SELECT f.source_file, f.current_page_results,
                   COUNT(r.event_id) AS loaded
            FROM raw_feed_files f
            LEFT JOIN raw_event_records r USING (source_file)
            GROUP BY 1,2
            HAVING f.current_page_results IS NOT NULL
               AND COUNT(r.event_id) != f.current_page_results
        )
        """,
        details_query="""
        SELECT f.source_file, f.current_page_results, COUNT(r.event_id) AS loaded
        FROM raw_feed_files f
        LEFT JOIN raw_event_records r USING (source_file)
        GROUP BY 1,2
        HAVING f.current_page_results IS NOT NULL
           AND COUNT(r.event_id) != f.current_page_results
        LIMIT 5
        """,
    )

    # Detect gaps in offset sequence
    qc.check(
        "pagination_offset_gaps",
        "WARNING",
        """
        WITH offsets AS (
            SELECT "offset",
                   LEAD("offset") OVER (ORDER BY "offset") AS next_offset,
                   limit_per_page
            FROM raw_feed_files
            WHERE "offset" IS NOT NULL
        )
        SELECT COUNT(*) FROM offsets
        WHERE next_offset IS NOT NULL
          AND limit_per_page IS NOT NULL
          AND next_offset - "offset" != limit_per_page
        """,
        details_query="""
        WITH offsets AS (
            SELECT "offset",
                   LEAD("offset") OVER (ORDER BY "offset") AS next_offset,
                   limit_per_page
            FROM raw_feed_files WHERE "offset" IS NOT NULL
        )
        SELECT "offset", next_offset, limit_per_page,
               next_offset - "offset" AS gap
        FROM offsets
        WHERE next_offset IS NOT NULL AND limit_per_page IS NOT NULL AND next_offset - "offset" != limit_per_page
        LIMIT 5
        """,
    )

    # ── Event identity checks ──────────────────────────────────────────────

    qc.check(
        "event_id_present",
        "ERROR",
        "SELECT COUNT(*) FROM raw_event_records WHERE event_id IS NULL",
    )

    qc.check(
        "event_id_unique_in_events_table",
        "ERROR",
        "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM events",
    )

    qc.check(
        "performer_slug_present",
        "WARNING",
        """
        SELECT COUNT(*) FROM raw_event_records r
        WHERE NOT EXISTS (
            SELECT 1 FROM event_performers ep WHERE ep.event_id = r.event_id
        )
        """,
    )

    qc.check(
        "venue_slug_present",
        "WARNING",
        """
        SELECT COUNT(*) FROM raw_event_records r
        WHERE NOT EXISTS (
            SELECT 1 FROM event_venues ev WHERE ev.event_id = r.event_id
        )
        """,
    )

    # ── Timestamp checks ───────────────────────────────────────────────────

    qc.check(
        "event_datetime_parseable",
        "ERROR",
        "SELECT COUNT(*) FROM events WHERE event_datetime IS NULL AND event_datetime_raw IS NOT NULL",
        details_query="SELECT event_id, event_datetime_raw FROM events WHERE event_datetime IS NULL AND event_datetime_raw IS NOT NULL LIMIT 5",
    )

    qc.check(
        "event_utc_datetime_parseable",
        "ERROR",
        "SELECT COUNT(*) FROM events WHERE event_utc_datetime IS NULL",
        details_query="SELECT event_id, title FROM events WHERE event_utc_datetime IS NULL LIMIT 5",
    )

    # Check that event_datetime and event_utc_datetime are within 24h of each other
    # (large discrepancy may indicate timezone handling issues)
    qc.check(
        "event_datetime_utc_consistency",
        "WARNING",
        """
        SELECT COUNT(*) FROM events
        WHERE event_datetime IS NOT NULL
          AND event_utc_datetime IS NOT NULL
          AND ABS(EPOCH(event_utc_datetime) - EPOCH(event_datetime)) > 86400
        """,
        details_query="""
        SELECT event_id, title, event_datetime, event_utc_datetime,
               ABS(EPOCH(event_utc_datetime) - EPOCH(event_datetime)) / 3600.0 AS diff_hours
        FROM events
        WHERE event_datetime IS NOT NULL AND event_utc_datetime IS NOT NULL
          AND ABS(EPOCH(event_utc_datetime) - EPOCH(event_datetime)) > 86400
        LIMIT 5
        """,
    )

    qc.check(
        "stats_last_updated_at_parseable",
        "ERROR",
        "SELECT COUNT(*) FROM event_market_state WHERE stats_last_updated_at IS NULL",
    )

    # ── Price checks ───────────────────────────────────────────────────────

    qc.check(
        "get_in_price_numeric",
        "ERROR",
        "SELECT COUNT(*) FROM event_market_state WHERE get_in_price IS NULL",
    )

    qc.check(
        "get_in_price_zero_flagged",
        "WARNING",
        "SELECT COUNT(*) FROM event_market_state WHERE get_in_price = 0",
        details_query="""
        SELECT e.event_id, e.title, e.category_type
        FROM event_market_state ms JOIN events e USING (event_id)
        WHERE ms.get_in_price = 0
        LIMIT 5
        """,
    )

    qc.check(
        "ticket_quantity_non_negative",
        "ERROR",
        "SELECT COUNT(*) FROM event_market_state WHERE stats_total_quantity < 0",
    )

    # ── Price change consistency ───────────────────────────────────────────

    # direction vs raw_change sign agreement
    qc.check(
        "price_change_direction_sign_match",
        "WARNING",
        """
        SELECT COUNT(*) FROM event_price_change_windows
        WHERE insufficient_data = false
          AND NOT (
              (direction = 'up'   AND raw_change > 0)
           OR (direction = 'down' AND raw_change < 0)
           OR (direction = 'flat' AND raw_change = 0)
          )
        """,
        details_query="""
        SELECT event_id, window_days, direction, raw_change, percent_change
        FROM event_price_change_windows
        WHERE insufficient_data = false
          AND NOT (
              (direction = 'up'   AND raw_change > 0)
           OR (direction = 'down' AND raw_change < 0)
           OR (direction = 'flat' AND raw_change = 0)
          )
        LIMIT 10
        """,
    )

    qc.check(
        "price_change_insufficient_data_handled",
        "INFO",
        """
        SELECT COUNT(*) FROM event_price_change_windows
        WHERE insufficient_data = true AND (raw_change != 0 OR percent_change != 0)
        """,
        details_query="""
        SELECT event_id, window_days, raw_change, percent_change
        FROM event_price_change_windows
        WHERE insufficient_data = true AND (raw_change != 0 OR percent_change != 0)
        LIMIT 5
        """,
    )

    qc.check(
        "suspicious_large_price_change",
        "WARNING",
        f"""
        SELECT COUNT(*) FROM event_price_change_windows
        WHERE insufficient_data = false
          AND ABS(percent_change) > {SUSPICIOUS_PRICE_PCT}
        """,
        details_query=f"""
        SELECT e.title, w.window_days, w.direction, w.percent_change, w.raw_change
        FROM event_price_change_windows w JOIN events e USING (event_id)
        WHERE w.insufficient_data = false AND ABS(w.percent_change) > {SUSPICIOUS_PRICE_PCT}
        ORDER BY ABS(w.percent_change) DESC
        LIMIT 10
        """,
    )

    # ── Forecast sentinel checks ───────────────────────────────────────────

    qc.check(
        "forecast_sentinel_not_treated_as_prediction",
        "WARNING",
        f"""
        SELECT COUNT(*) FROM event_market_state
        WHERE forecast_is_available = true
          AND forecast_value IN {SENTINEL_FORECAST_VALUES}
        """,
        details_query=f"""
        SELECT e.title, ms.forecast_value, ms.forecast_is_available
        FROM event_market_state ms JOIN events e USING (event_id)
        WHERE ms.forecast_is_available = true AND ms.forecast_value IN {SENTINEL_FORECAST_VALUES}
        LIMIT 5
        """,
    )

    # Events with forecast_is_available = false should have sentinel values
    qc.check(
        "forecast_unavailable_has_sentinel",
        "INFO",
        f"""
        SELECT COUNT(*) FROM event_market_state
        WHERE forecast_is_available = false
          AND forecast_value NOT IN {SENTINEL_FORECAST_VALUES}
          AND forecast_value IS NOT NULL
        """,
    )

    # ── Freshness checks ───────────────────────────────────────────────────

    qc.check(
        "stats_last_updated_stale",
        "WARNING",
        f"""
        SELECT COUNT(*) FROM event_market_state
        WHERE stats_last_updated_at < (CURRENT_TIMESTAMP - INTERVAL '{STALE_HOURS} hours')
        """,
        details_query=f"""
        SELECT e.title, ms.stats_last_updated_at,
               EPOCH(CURRENT_TIMESTAMP - ms.stats_last_updated_at) / 3600.0 AS hours_old
        FROM event_market_state ms JOIN events e USING (event_id)
        WHERE ms.stats_last_updated_at < (CURRENT_TIMESTAMP - INTERVAL '48 hours')
        ORDER BY ms.stats_last_updated_at ASC
        LIMIT 5
        """,
    )

    # ── Mock data check ────────────────────────────────────────────────────

    qc.check(
        "mock_data_points_present",
        "WARNING",
        "SELECT COUNT(*) FROM event_market_state WHERE mock_data_point != 0",
        details_query="""
        SELECT e.title, ms.mock_data_point
        FROM event_market_state ms JOIN events e USING (event_id)
        WHERE ms.mock_data_point != 0
        LIMIT 5
        """,
    )

    qc.summary()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="RTK Tickets data quality checks")
    parser.add_argument("--database", required=True, help="Path to DuckDB database file")
    args = parser.parse_args()
    run_checks(args.database)


if __name__ == "__main__":
    main()
