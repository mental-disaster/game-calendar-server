CREATE TABLE IF NOT EXISTS service.etl_run_log (
    run_id          UUID        NOT NULL PRIMARY KEY,
    trigger_type    TEXT        NOT NULL,
    ingest_sync_id  UUID,
    status          TEXT        NOT NULL DEFAULT 'running',
    mismatch_count  INT         NOT NULL DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_service_etl_run_log_started_at
    ON service.etl_run_log (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_service_etl_run_log_ingest_sync_id
    ON service.etl_run_log (ingest_sync_id);


CREATE TABLE IF NOT EXISTS service.etl_source_log (
    id              BIGSERIAL   PRIMARY KEY,
    run_id          UUID        NOT NULL REFERENCES service.etl_run_log (run_id) ON DELETE CASCADE,
    table_name      TEXT        NOT NULL,
    status          TEXT        NOT NULL,
    processed_rows  INT         NOT NULL DEFAULT 0,
    cursor_from     BIGINT,
    cursor_to       BIGINT,
    note            TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL,
    UNIQUE (run_id, table_name)
);

CREATE INDEX IF NOT EXISTS idx_service_etl_source_log_run_id
    ON service.etl_source_log (run_id);

CREATE INDEX IF NOT EXISTS idx_service_etl_source_log_table_name
    ON service.etl_source_log (table_name);


CREATE TABLE IF NOT EXISTS service.etl_mismatch_log (
    id              BIGSERIAL   PRIMARY KEY,
    run_id          UUID        NOT NULL REFERENCES service.etl_run_log (run_id) ON DELETE CASCADE,
    table_name      TEXT,
    mismatch_type   TEXT        NOT NULL,
    expected_value  TEXT,
    actual_value    TEXT,
    details         JSONB,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_service_etl_mismatch_log_run_id
    ON service.etl_mismatch_log (run_id);


CREATE TABLE IF NOT EXISTS service.etl_cursor (
    table_name      TEXT        NOT NULL PRIMARY KEY,
    last_synced_at  BIGINT      NOT NULL,
    synced_at       TIMESTAMPTZ NOT NULL
);
