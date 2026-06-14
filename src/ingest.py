"""
RTK Tickets feed ingestion pipeline.

Usage:
    python -m src.ingest --input data/raw --database output/ticket_feed.duckdb

Design notes:
- events['all'] is used as the canonical list per page (superset of past/upcoming).
- Idempotency: raw_event_records PK = file_id:event_id; dimensions upsert on
  natural keys; market state PK = run_id:event_id.
- Malformed records are skipped; errors recorded in data_quality_results.
- Files are processed in batches of BATCH_SIZE and committed per batch to avoid
  memory pressure on constrained environments.
- Timezone: event_datetime arrives as ISO string; event_utc_datetime is the
  confirmed UTC stamp. Both stored as TIMESTAMPTZ.
  Use event_utc_datetime for cross-timezone comparisons.
"""

import argparse
import glob
import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import duckdb

from src.schema import create_schema

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SENTINEL_FORECAST_VALUES = {-1, -100}
SUSPICIOUS_PRICE_CHANGE_PCT = 100.0
STALE_HOURS = 48
BATCH_SIZE = 10  # files per commit


def file_id(path: str) -> str:
    return hashlib.sha256(os.path.basename(path).encode()).hexdigest()[:16]


def safe_ts(value) -> str | None:
    if not value:
        return None
    try:
        if isinstance(value, str):
            return value.replace("Z", "+00:00")
        return None
    except Exception:
        return None


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def extract_offset_from_filename(path: str) -> int | None:
    base = os.path.basename(path)
    try:
        return int(base.split("offset-")[-1].replace(".json", ""))
    except (IndexError, ValueError):
        return None


def load_json_file(path: str) -> dict | None:
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error("Cannot load %s: %s", path, e)
        return None


def validate_feed_file(data: dict, path: str) -> list[str]:
    errors = []
    if data.get("status") != "success":
        errors.append(f"status != success (got {data.get('status')!r})")
    if "data" not in data:
        errors.append("missing top-level 'data' key")
        return errors
    if "metadata" not in data["data"]:
        errors.append("missing data.metadata")
    if "events" not in data["data"]:
        errors.append("missing data.events")
    return errors


def extract_events(data: dict) -> list[dict]:
    block = data.get("data", {}).get("events", {})
    if isinstance(block, dict):
        return block.get("all", [])
    if isinstance(block, list):
        return block
    return []


def extract_metadata(data: dict) -> dict:
    return data.get("data", {}).get("metadata", {})


def commit_batch(conn, run_id: str, feed_files: list, events: list,
                 performers: list, venues: list, ep: list, ev: list,
                 market: list, windows: list, raw_records: list) -> None:
    """Commit one batch of files atomically."""
    conn.execute("BEGIN")
    try:
        if feed_files:
            conn.executemany("""
                INSERT INTO raw_feed_files
                  (file_id, run_id, source_file, "offset", feed_status,
                   total_db_matches, current_page_results, has_more, limit_per_page,
                   tab_requested, date_filter_start, date_filter_end,
                   upcoming_count, past_count, total_count, loaded_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT (file_id) DO UPDATE SET
                  run_id=excluded.run_id, loaded_at=excluded.loaded_at
            """, feed_files)

        if events:
            conn.executemany("""
                INSERT INTO events
                  (event_id, title, category_type, event_category_name,
                   event_datetime, event_datetime_raw, event_timezone,
                   event_utc_datetime, date_display, first_seen_at, last_seen_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT (event_id) DO UPDATE SET
                  last_seen_at=excluded.last_seen_at,
                  title=excluded.title,
                  category_type=excluded.category_type,
                  event_category_name=excluded.event_category_name
            """, events)

        if performers:
            conn.executemany("""
                INSERT INTO performers (performer_slug, performer_name, first_seen_at, last_seen_at)
                VALUES (?,?,?,?)
                ON CONFLICT (performer_slug) DO UPDATE SET last_seen_at=excluded.last_seen_at
            """, performers)

        if ep:
            conn.executemany("""
                INSERT INTO event_performers (event_id, performer_slug) VALUES (?,?)
                ON CONFLICT DO NOTHING
            """, ep)

        if venues:
            conn.executemany("""
                INSERT INTO venues (venue_slug, venue_name, city, state, country, first_seen_at, last_seen_at)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT (venue_slug) DO UPDATE SET last_seen_at=excluded.last_seen_at
            """, venues)

        if ev:
            conn.executemany("""
                INSERT INTO event_venues (event_id, venue_slug) VALUES (?,?)
                ON CONFLICT DO NOTHING
            """, ev)

        if market:
            conn.executemany("""
                INSERT INTO event_market_state
                  (state_id, run_id, event_id, source_file,
                   get_in_price, stats_total_quantity, stats_last_updated_at,
                   forecast_is_available, forecast_value,
                   forecast_layover_text, forecast_hover_text,
                   is_past_with_no_pricing, mock_data_point,
                   disable_click_through, loaded_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT (state_id) DO NOTHING
            """, market)

        if windows:
            conn.executemany("""
                INSERT INTO event_price_change_windows
                  (window_id, run_id, event_id, window_days,
                   direction, percent_change, raw_change, insufficient_data)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT (window_id) DO NOTHING
            """, windows)

        # raw_event_records in smaller sub-chunks (large blobs)
        if raw_records:
            sub = 200
            sql = """
                INSERT INTO raw_event_records
                  (raw_id, run_id, source_file, source_offset, event_id, raw_json, loaded_at)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT (raw_id) DO NOTHING
            """
            for i in range(0, len(raw_records), sub):
                conn.executemany(sql, raw_records[i:i+sub])

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def process_file(path: str, data: dict, run_id: str, malformed_events: list):
    """Return (n_loaded, feed_file_row, events, performers, venues, ep, ev, market, windows, raw)."""
    meta = extract_metadata(data)
    pag  = meta.get("pagination", {})
    cat  = meta.get("categorization", {})
    dr   = meta.get("filters", {}).get("date_range", {})
    fid  = file_id(path)
    ts   = now_utc()

    feed_row = (
        fid, run_id, os.path.basename(path),
        extract_offset_from_filename(path),
        data.get("status"),
        pag.get("total_database_matches"),
        pag.get("current_page_results"),
        bool(pag.get("has_more")),
        pag.get("limit"),
        pag.get("tab_requested"),
        dr.get("start_date"),
        dr.get("end_date"),
        cat.get("upcoming_count"),
        cat.get("past_count"),
        cat.get("total_count"),
        ts,
    )

    evts_out, performers, venues, ep, ev, market, windows, raw = [], [], [], [], [], [], [], []
    loaded = 0

    for evt in extract_events(data):
        eid = evt.get("id")
        if not eid:
            malformed_events.append((path, "missing id", ""))
            continue
        try:
            evts_out.append((
                eid, evt.get("title"), evt.get("category_type"),
                evt.get("event_category_name"),
                safe_ts(evt.get("event_datetime")), evt.get("event_datetime"),
                evt.get("event_timezone"),
                safe_ts(evt.get("event_utc_datetime")),
                evt.get("date"), ts, ts,
            ))

            pslug = evt.get("performer_slug")
            pname = evt.get("performer")
            if pslug and pname:
                performers.append((pslug, pname, ts, ts))
                ep.append((eid, pslug))

            vslug = evt.get("venue_slug")
            vname = evt.get("venue")
            if vslug and vname:
                venues.append((vslug, vname, evt.get("city"), evt.get("state"), evt.get("country"), ts, ts))
                ev.append((eid, vslug))

            market.append((
                f"{run_id}:{eid}", run_id, eid, os.path.basename(path),
                evt.get("get_in_price"),
                evt.get("stats_total_quantity_of_tickets"),
                safe_ts(evt.get("stats_last_updated_at")),
                evt.get("forecast_is_available"),
                evt.get("forecast_value"),
                evt.get("forecast_layover_text"),
                evt.get("forecast_hover_text"),
                evt.get("is_past_with_no_pricing"),
                evt.get("mock_data_point"),
                evt.get("disable_click_through"),
                ts,
            ))

            for days in [3, 7, 14, 30]:
                pc = evt.get(f"{days}day_price_change") or {}
                if pc:
                    windows.append((
                        f"{run_id}:{eid}:{days}", run_id, eid, days,
                        pc.get("direction"), pc.get("percent"), pc.get("raw"),
                        bool(pc.get("insufficient_data", False)),
                    ))

            raw.append((
                f"{fid}:{eid}", run_id, os.path.basename(path),
                extract_offset_from_filename(path),
                eid, json.dumps(evt), ts,
            ))
            loaded += 1
        except Exception as e:
            log.warning("Error evt %s in %s: %s", eid, os.path.basename(path), e)
            malformed_events.append((path, str(e), str(eid)))

    return loaded, feed_row, evts_out, performers, venues, ep, ev, market, windows, raw


def ingest(input_dir: str, database: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(database)), exist_ok=True)
    conn = duckdb.connect(database)
    create_schema(conn)

    run_id = str(uuid.uuid4())
    conn.execute("""
        INSERT INTO ingestion_runs (run_id, started_at, input_dir, status)
        VALUES (?,?,?,'running') ON CONFLICT (run_id) DO NOTHING
    """, [run_id, now_utc(), input_dir])
    log.info("Run %s started", run_id)

    files = sorted(glob.glob(os.path.join(input_dir, "*.json")))
    log.info("Found %d JSON files in %s", len(files), input_dir)

    files_loaded = 0
    events_loaded = 0
    malformed_files = []
    malformed_events = []

    for batch_start in range(0, len(files), BATCH_SIZE):
        batch_files = files[batch_start:batch_start + BATCH_SIZE]
        b_feeds, b_evts, b_perf, b_venues = [], [], [], []
        b_ep, b_ev, b_market, b_windows, b_raw = [], [], [], [], []

        for path in batch_files:
            data = load_json_file(path)
            if data is None:
                malformed_files.append((path, "JSON parse error"))
                continue
            errors = validate_feed_file(data, path)
            if errors:
                log.warning("Skipping %s: %s", os.path.basename(path), "; ".join(errors))
                malformed_files.append((path, "; ".join(errors)))
                continue

            n, feed_row, evts, perf, venues, ep, ev, market, windows, raw = \
                process_file(path, data, run_id, malformed_events)

            b_feeds.append(feed_row)
            b_evts.extend(evts)
            b_perf.extend(perf)
            b_venues.extend(venues)
            b_ep.extend(ep)
            b_ev.extend(ev)
            b_market.extend(market)
            b_windows.extend(windows)
            b_raw.extend(raw)
            events_loaded += n
            files_loaded += 1

        if b_feeds:
            commit_batch(conn, run_id, b_feeds, b_evts, b_perf, b_venues,
                         b_ep, b_ev, b_market, b_windows, b_raw)
            log.info("  Committed batch offsets %d-%d (%d events)",
                     batch_start, batch_start + len(batch_files) - 1, len(b_evts))

    conn.execute("""
        UPDATE ingestion_runs
        SET finished_at=?, files_found=?, files_loaded=?, events_loaded=?, status='success'
        WHERE run_id=?
    """, [now_utc(), len(files), files_loaded, events_loaded, run_id])

    _write_ingest_dq(conn, run_id, malformed_files, malformed_events)
    conn.close()

    log.info("Run %s complete: %d/%d files, %d events loaded",
             run_id, files_loaded, len(files), events_loaded)
    if malformed_files:
        log.warning("%d files had errors", len(malformed_files))
    if malformed_events:
        log.warning("%d events skipped", len(malformed_events))


def _write_ingest_dq(conn, run_id, malformed_files, malformed_events):
    ts = now_utc()
    rows = [
        (f"{run_id}:malformed_files", run_id, "malformed_files", "ERROR",
         "FAIL" if malformed_files else "PASS", len(malformed_files),
         "; ".join(f"{os.path.basename(p)}: {e}" for p, e in malformed_files) or None, ts),
        (f"{run_id}:malformed_events", run_id, "malformed_events", "ERROR",
         "FAIL" if malformed_events else "PASS", len(malformed_events),
         "; ".join(f"{os.path.basename(p)} evt={eid}: {e}" for p, e, eid in malformed_events) or None, ts),
    ]
    conn.executemany("""
        INSERT INTO data_quality_results
          (check_id, run_id, check_name, severity, status, affected_count, details, checked_at)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT (check_id) DO NOTHING
    """, rows)


def main():
    parser = argparse.ArgumentParser(description="RTK Tickets feed ingestion")
    parser.add_argument("--input", required=True, help="Directory of raw JSON files")
    parser.add_argument("--database", required=True, help="Path to DuckDB database file")
    args = parser.parse_args()
    ingest(args.input, args.database)


if __name__ == "__main__":
    main()
