"""
Schema creation for the RTK ticket feed pipeline.
All tables are created with IF NOT EXISTS for idempotency.
DuckDB does not support executescript; statements are split and run individually.
'offset' is a reserved word in DuckDB and must be double-quoted.
"""

_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS ingestion_runs (
        run_id          TEXT PRIMARY KEY,
        started_at      TIMESTAMPTZ NOT NULL,
        finished_at     TIMESTAMPTZ,
        input_dir       TEXT NOT NULL,
        files_found     INTEGER,
        files_loaded    INTEGER,
        events_loaded   INTEGER,
        status          TEXT DEFAULT 'running'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_feed_files (
        file_id              TEXT PRIMARY KEY,
        run_id               TEXT NOT NULL REFERENCES ingestion_runs(run_id),
        source_file          TEXT NOT NULL,
        "offset"             INTEGER,
        feed_status          TEXT,
        total_db_matches     INTEGER,
        current_page_results INTEGER,
        has_more             BOOLEAN,
        limit_per_page       INTEGER,
        tab_requested        TEXT,
        date_filter_start    DATE,
        date_filter_end      DATE,
        upcoming_count       INTEGER,
        past_count           INTEGER,
        total_count          INTEGER,
        loaded_at            TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_event_records (
        raw_id        TEXT PRIMARY KEY,
        run_id        TEXT NOT NULL,
        source_file   TEXT NOT NULL,
        source_offset INTEGER,
        event_id      BIGINT NOT NULL,
        raw_json      TEXT NOT NULL,
        loaded_at     TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        event_id            BIGINT PRIMARY KEY,
        title               TEXT,
        category_type       TEXT,
        event_category_name TEXT,
        event_datetime      TIMESTAMPTZ,
        event_datetime_raw  TEXT,
        event_timezone      TEXT,
        event_utc_datetime  TIMESTAMPTZ,
        date_display        TEXT,
        first_seen_at       TIMESTAMPTZ NOT NULL,
        last_seen_at        TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS performers (
        performer_slug TEXT PRIMARY KEY,
        performer_name TEXT NOT NULL,
        first_seen_at  TIMESTAMPTZ NOT NULL,
        last_seen_at   TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS venues (
        venue_slug    TEXT PRIMARY KEY,
        venue_name    TEXT NOT NULL,
        city          TEXT,
        state         TEXT,
        country       TEXT,
        first_seen_at TIMESTAMPTZ NOT NULL,
        last_seen_at  TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS event_performers (
        event_id       BIGINT NOT NULL REFERENCES events(event_id),
        performer_slug TEXT NOT NULL REFERENCES performers(performer_slug),
        PRIMARY KEY (event_id, performer_slug)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS event_venues (
        event_id   BIGINT NOT NULL REFERENCES events(event_id),
        venue_slug TEXT NOT NULL REFERENCES venues(venue_slug),
        PRIMARY KEY (event_id, venue_slug)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS event_market_state (
        state_id                TEXT PRIMARY KEY,
        run_id                  TEXT NOT NULL REFERENCES ingestion_runs(run_id),
        event_id                BIGINT NOT NULL,
        source_file             TEXT NOT NULL,
        get_in_price            DOUBLE,
        stats_total_quantity    INTEGER,
        stats_last_updated_at   TIMESTAMPTZ,
        forecast_is_available   BOOLEAN,
        forecast_value          DOUBLE,
        forecast_layover_text   TEXT,
        forecast_hover_text     TEXT,
        is_past_with_no_pricing BOOLEAN,
        mock_data_point         INTEGER,
        disable_click_through   INTEGER,
        loaded_at               TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS event_price_change_windows (
        window_id         TEXT PRIMARY KEY,
        run_id            TEXT NOT NULL,
        event_id          BIGINT NOT NULL,
        window_days       INTEGER NOT NULL,
        direction         TEXT,
        percent_change    DOUBLE,
        raw_change        DOUBLE,
        insufficient_data BOOLEAN
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS data_quality_results (
        check_id       TEXT PRIMARY KEY,
        run_id         TEXT NOT NULL REFERENCES ingestion_runs(run_id),
        check_name     TEXT NOT NULL,
        severity       TEXT NOT NULL,
        status         TEXT NOT NULL,
        affected_count INTEGER DEFAULT 0,
        details        TEXT,
        checked_at     TIMESTAMPTZ NOT NULL
    )
    """,
]


def create_schema(conn):
    """Create all tables. Safe to call multiple times (idempotent)."""
    for stmt in _STATEMENTS:
        conn.execute(stmt.strip())
