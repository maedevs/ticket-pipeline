"""
Tests for the RTK Tickets feed pipeline.

Run with:  pytest tests/ -v
"""

import json
import os
import tempfile

import duckdb
import pytest

from src.ingest import (
    extract_events,
    extract_metadata,
    extract_offset_from_filename,
    file_id,
    ingest,
    load_json_file,
    safe_ts,
    validate_feed_file,
)
from src.schema import create_schema


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────

def make_event(overrides: dict = None) -> dict:
    """Build a minimal valid event record."""
    evt = {
        "id": 12345,
        "title": "Test Event",
        "performer": "Test Performer",
        "performer_slug": "test-performer",
        "venue": "Test Venue",
        "venue_slug": "test-venue",
        "city": "New York",
        "state": "NY",
        "country": "US",
        "date": "Mon, 06/01/2026 07:30 PM",
        "event_datetime": "2026-06-01T19:30:00Z",
        "event_timezone": "EDT",
        "event_utc_datetime": "2026-06-01T23:30:00Z",
        "category_type": "CONCERT",
        "event_category_name": "Pop",
        "get_in_price": 150.0,
        "stats_total_quantity_of_tickets": 200,
        "stats_last_updated_at": "2026-05-27T10:00:00Z",
        "forecast_is_available": True,
        "forecast_value": 0.75,
        "forecast_layover_text": "Prices likely to rise",
        "forecast_hover_text": "75% confidence",
        "is_past_with_no_pricing": False,
        "mock_data_point": 0,
        "disable_click_through": 0,
        "3day_price_change": {"direction": "up", "percent": 5.0, "raw": 7.50, "insufficient_data": False},
        "7day_price_change": {"direction": "up", "percent": 10.0, "raw": 15.0, "insufficient_data": False},
        "14day_price_change": {"direction": "flat", "percent": 0, "raw": 0, "insufficient_data": False},
        "30day_price_change": {"direction": "down", "percent": -2.0, "raw": -3.0, "insufficient_data": False},
    }
    if overrides:
        evt.update(overrides)
    return evt


def make_feed_file(events: list = None, status: str = "success", offset: int = 0) -> dict:
    """Build a minimal valid feed file payload."""
    return {
        "status": status,
        "data": {
            "metadata": {
                "categorization": {
                    "current_utc": "2026-05-27T17:00:00Z",
                    "total_count": len(events or []),
                    "upcoming_count": len(events or []),
                    "past_count": 0,
                },
                "filters": {
                    "date_range": {
                        "start_date": "2026-05-27",
                        "end_date": "2026-06-26",
                        "is_any": False,
                    }
                },
                "pagination": {
                    "offset": offset,
                    "limit": 100,
                    "current_page_results": len(events or []),
                    "has_more": False,
                    "tab_requested": None,
                    "total_database_matches": len(events or []),
                },
                "performance_timing": {"total_seconds": 1.0},
            },
            "events": {"all": events or [], "past": [], "upcoming": events or []},
        },
    }


@pytest.fixture
def tmp_db():
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "test.duckdb")
        conn = duckdb.connect(db_path)
        create_schema(conn)
        yield conn, db_path
        conn.close()


@pytest.fixture
def tmp_input_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


# ──────────────────────────────────────────────────────────────────────────────
# Unit tests — helpers
# ──────────────────────────────────────────────────────────────────────────────

class TestHelpers:
    def test_extract_offset_valid(self):
        assert extract_offset_from_filename("2026-05-27_to_2026-06-26__offset-1000.json") == 1000

    def test_extract_offset_zero(self):
        assert extract_offset_from_filename("2026-05-27_to_2026-06-26__offset-0.json") == 0

    def test_extract_offset_no_match(self):
        assert extract_offset_from_filename("some_other_file.json") is None

    def test_file_id_deterministic(self):
        assert file_id("foo.json") == file_id("foo.json")
        assert file_id("foo.json") != file_id("bar.json")

    def test_safe_ts_valid(self):
        result = safe_ts("2026-06-01T19:30:00Z")
        assert result == "2026-06-01T19:30:00+00:00"

    def test_safe_ts_none(self):
        assert safe_ts(None) is None
        assert safe_ts("") is None

    def test_safe_ts_already_offset(self):
        result = safe_ts("2026-06-01T19:30:00+00:00")
        assert result == "2026-06-01T19:30:00+00:00"


# ──────────────────────────────────────────────────────────────────────────────
# Unit tests — validation
# ──────────────────────────────────────────────────────────────────────────────

class TestValidation:
    def test_valid_feed_passes(self):
        data = make_feed_file([make_event()])
        errors = validate_feed_file(data, "test.json")
        assert errors == []

    def test_missing_status_fails(self):
        data = make_feed_file()
        data["status"] = "error"
        errors = validate_feed_file(data, "test.json")
        assert any("status" in e for e in errors)

    def test_missing_data_key_fails(self):
        errors = validate_feed_file({"status": "success"}, "test.json")
        assert any("data" in e for e in errors)

    def test_missing_events_fails(self):
        data = make_feed_file()
        del data["data"]["events"]
        errors = validate_feed_file(data, "test.json")
        assert any("events" in e for e in errors)


# ──────────────────────────────────────────────────────────────────────────────
# Unit tests — extraction
# ──────────────────────────────────────────────────────────────────────────────

class TestExtraction:
    def test_extract_events_from_dict(self):
        evt = make_event()
        data = make_feed_file([evt])
        events = extract_events(data)
        assert len(events) == 1
        assert events[0]["id"] == evt["id"]

    def test_extract_events_empty(self):
        data = make_feed_file([])
        assert extract_events(data) == []

    def test_extract_events_list_fallback(self):
        """If events is a bare list (not dict), still works."""
        data = make_feed_file()
        data["data"]["events"] = [make_event()]
        assert len(extract_events(data)) == 1

    def test_extract_metadata(self):
        data = make_feed_file([make_event()])
        meta = extract_metadata(data)
        assert "pagination" in meta
        assert meta["pagination"]["offset"] == 0


# ──────────────────────────────────────────────────────────────────────────────
# Integration tests — full ingest pipeline
# ──────────────────────────────────────────────────────────────────────────────

class TestIngest:
    def _write_feed(self, directory: str, filename: str, data: dict) -> str:
        path = os.path.join(directory, filename)
        with open(path, "w") as f:
            json.dump(data, f)
        return path

    def test_basic_ingest_populates_tables(self, tmp_input_dir, tmp_db):
        _, db_path = tmp_db
        evt = make_event()
        data = make_feed_file([evt])
        self._write_feed(tmp_input_dir, "2026-05-27_to_2026-06-26__offset-0.json", data)

        ingest(tmp_input_dir, db_path)

        conn = duckdb.connect(db_path)
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM performers").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM venues").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM event_market_state").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM event_price_change_windows").fetchone()[0] == 4
        assert conn.execute("SELECT COUNT(*) FROM raw_event_records").fetchone()[0] == 1
        conn.close()

    def test_idempotency(self, tmp_input_dir, tmp_db):
        """Running ingest twice on the same files should not duplicate records."""
        _, db_path = tmp_db
        data = make_feed_file([make_event()])
        self._write_feed(tmp_input_dir, "2026-05-27_to_2026-06-26__offset-0.json", data)

        ingest(tmp_input_dir, db_path)
        ingest(tmp_input_dir, db_path)

        conn = duckdb.connect(db_path)
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM raw_event_records").fetchone()[0] == 1
        conn.close()

    def test_missing_event_id_skipped(self, tmp_input_dir, tmp_db):
        _, db_path = tmp_db
        bad_evt = make_event({"id": None})
        data = make_feed_file([bad_evt])
        self._write_feed(tmp_input_dir, "2026-05-27_to_2026-06-26__offset-0.json", data)

        ingest(tmp_input_dir, db_path)

        conn = duckdb.connect(db_path)
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
        conn.close()

    def test_malformed_json_skipped_gracefully(self, tmp_input_dir, tmp_db):
        _, db_path = tmp_db
        bad_path = os.path.join(tmp_input_dir, "2026-05-27_to_2026-06-26__offset-0.json")
        with open(bad_path, "w") as f:
            f.write("{not valid json{{")

        ingest(tmp_input_dir, db_path)  # should not raise

        conn = duckdb.connect(db_path)
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 0
        conn.close()

    def test_multiple_files_loaded(self, tmp_input_dir, tmp_db):
        _, db_path = tmp_db
        for offset, eid in [(0, 111), (100, 222), (200, 333)]:
            data = make_feed_file([make_event({"id": eid})], offset=offset)
            self._write_feed(
                tmp_input_dir,
                f"2026-05-27_to_2026-06-26__offset-{offset}.json",
                data,
            )

        ingest(tmp_input_dir, db_path)

        conn = duckdb.connect(db_path)
        assert conn.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 3
        conn.close()

    def test_run_recorded_in_ingestion_runs(self, tmp_input_dir, tmp_db):
        _, db_path = tmp_db
        data = make_feed_file([make_event()])
        self._write_feed(tmp_input_dir, "2026-05-27_to_2026-06-26__offset-0.json", data)

        ingest(tmp_input_dir, db_path)

        conn = duckdb.connect(db_path)
        row = conn.execute(
            "SELECT status, files_loaded, events_loaded FROM ingestion_runs LIMIT 1"
        ).fetchone()
        assert row[0] == "success"
        assert row[1] == 1
        assert row[2] == 1
        conn.close()

    def test_price_change_windows_direction(self, tmp_input_dir, tmp_db):
        """All 4 windows should be inserted with correct direction."""
        _, db_path = tmp_db
        data = make_feed_file([make_event()])
        self._write_feed(tmp_input_dir, "2026-05-27_to_2026-06-26__offset-0.json", data)

        ingest(tmp_input_dir, db_path)

        conn = duckdb.connect(db_path)
        rows = conn.execute(
            "SELECT window_days, direction FROM event_price_change_windows ORDER BY window_days"
        ).fetchall()
        assert len(rows) == 4
        directions = {r[0]: r[1] for r in rows}
        assert directions[3] == "up"
        assert directions[7] == "up"
        assert directions[14] == "flat"
        assert directions[30] == "down"
        conn.close()

    def test_zero_price_event_ingested(self, tmp_input_dir, tmp_db):
        _, db_path = tmp_db
        data = make_feed_file([make_event({"get_in_price": 0})])
        self._write_feed(tmp_input_dir, "2026-05-27_to_2026-06-26__offset-0.json", data)

        ingest(tmp_input_dir, db_path)

        conn = duckdb.connect(db_path)
        price = conn.execute("SELECT get_in_price FROM event_market_state").fetchone()[0]
        assert price == 0
        conn.close()

    def test_sentinel_forecast_ingested_without_error(self, tmp_input_dir, tmp_db):
        _, db_path = tmp_db
        data = make_feed_file([make_event({"forecast_is_available": False, "forecast_value": -100})])
        self._write_feed(tmp_input_dir, "2026-05-27_to_2026-06-26__offset-0.json", data)

        ingest(tmp_input_dir, db_path)

        conn = duckdb.connect(db_path)
        fv = conn.execute("SELECT forecast_value FROM event_market_state").fetchone()[0]
        assert fv == -100
        conn.close()
