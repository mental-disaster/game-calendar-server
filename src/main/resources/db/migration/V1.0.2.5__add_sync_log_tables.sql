-- sync 실행 로그 (1회 syncAll = 1행)
CREATE TABLE IF NOT EXISTS ingest.sync_log (
    sync_id     UUID        NOT NULL PRIMARY KEY,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status      TEXT        NOT NULL DEFAULT 'running'  -- 'running' | 'completed' | 'failed'
);

-- 테이블별 처리 결과 (1 sync × 1 table = 1행)
CREATE TABLE IF NOT EXISTS ingest.sync_table_log (
    id          BIGSERIAL   PRIMARY KEY,
    sync_id     UUID        NOT NULL,
    table_name  TEXT        NOT NULL,
    fetched     INT         NOT NULL DEFAULT 0,
    upserted    INT         NOT NULL DEFAULT 0,
    errors      INT         NOT NULL DEFAULT 0,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL
);

-- 파싱 실패 레코드 상세 (1 실패 = 1행)
CREATE TABLE IF NOT EXISTS ingest.sync_parse_error (
    id          BIGSERIAL   PRIMARY KEY,
    sync_id     UUID        NOT NULL,
    table_name  TEXT        NOT NULL,
    record_id   BIGINT,
    raw_json    JSONB,
    error_msg   TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
