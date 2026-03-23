-- ingest 스키마 업데이트
-- 변경 내용:
--   1. 모든 테이블에 PRIMARY KEY 추가 (upsert ON CONFLICT 지원)
--   2. sync_cursor 테이블 추가 (IGDB updated_at 기반 증분 수집 커서 관리)


-- ============================================================================
-- 1. PRIMARY KEY 추가
-- ============================================================================

ALTER TABLE ingest.game                 ADD PRIMARY KEY (id);
ALTER TABLE ingest.alternative_name     ADD PRIMARY KEY (id);
ALTER TABLE ingest.game_localization    ADD PRIMARY KEY (id);
ALTER TABLE ingest.region               ADD PRIMARY KEY (id);
ALTER TABLE ingest.release_date         ADD PRIMARY KEY (id);
ALTER TABLE ingest.release_date_region  ADD PRIMARY KEY (id);
ALTER TABLE ingest.release_date_status  ADD PRIMARY KEY (id);
ALTER TABLE ingest.platform             ADD PRIMARY KEY (id);
ALTER TABLE ingest.platform_logo        ADD PRIMARY KEY (id);
ALTER TABLE ingest.platform_type        ADD PRIMARY KEY (id);
ALTER TABLE ingest.game_status          ADD PRIMARY KEY (id);
ALTER TABLE ingest.game_type            ADD PRIMARY KEY (id);
ALTER TABLE ingest.language_support     ADD PRIMARY KEY (id);
ALTER TABLE ingest.language             ADD PRIMARY KEY (id);
ALTER TABLE ingest.language_support_type ADD PRIMARY KEY (id);
ALTER TABLE ingest.genre                ADD PRIMARY KEY (id);
ALTER TABLE ingest.theme                ADD PRIMARY KEY (id);
ALTER TABLE ingest.player_perspective   ADD PRIMARY KEY (id);
ALTER TABLE ingest.game_mode            ADD PRIMARY KEY (id);
ALTER TABLE ingest.keyword              ADD PRIMARY KEY (id);
ALTER TABLE ingest.involved_company     ADD PRIMARY KEY (id);
ALTER TABLE ingest.company              ADD PRIMARY KEY (id);
ALTER TABLE ingest.cover                ADD PRIMARY KEY (id);
ALTER TABLE ingest.artwork              ADD PRIMARY KEY (id);
ALTER TABLE ingest.screenshot           ADD PRIMARY KEY (id);
ALTER TABLE ingest.game_video           ADD PRIMARY KEY (id);
ALTER TABLE ingest.website              ADD PRIMARY KEY (id);
ALTER TABLE ingest.website_type         ADD PRIMARY KEY (id);


-- ============================================================================
-- 2. sync_cursor 테이블 추가
--   - 테이블별 마지막 수집 시각(IGDB updated_at, Unix timestamp)을 저장
--   - last_synced_at = 0 이면 전체 수집, 이후부터는 변경분만 수집
--   - updated_at 을 제공하지 않는 테이블(cover, artwork, screenshot 등)은
--     연관된 game 의 커서를 따라가므로 별도 row 불필요
-- ============================================================================

CREATE TABLE IF NOT EXISTS ingest.sync_cursor (
    table_name      TEXT        PRIMARY KEY,
    last_synced_at  BIGINT      NOT NULL DEFAULT 0,  -- IGDB updated_at (Unix timestamp)
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE ingest.sync_cursor IS '테이블별 IGDB 증분 수집 커서';
COMMENT ON COLUMN ingest.sync_cursor.table_name     IS 'ingest 테이블명';
COMMENT ON COLUMN ingest.sync_cursor.last_synced_at IS '마지막으로 수집한 IGDB updated_at (Unix timestamp), 0이면 전체 수집';
COMMENT ON COLUMN ingest.sync_cursor.synced_at      IS '커서 마지막 갱신 시각';
