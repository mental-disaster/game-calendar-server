-- 증분 동기화 쿼리 성능을 위한 복합 인덱스
--
-- 사용 쿼리:
--   1. 미디어 증분 동기화 (findAllIdsUpdatedAfter)
--      WHERE updated_at > :updatedAfter AND id > :lastId ORDER BY id ASC
--   2. 커서 기반 동기화 (IgdbClient buildQuery)
--      WHERE updated_at > :cursor AND id > :lastId ORDER BY id ASC

-- game: 미디어 증분 동기화 + 커서 기반 동기화
CREATE INDEX IF NOT EXISTS idx_ingest_game_updated_at_id
    ON ingest.game (updated_at, id);

-- release_date: 커서 기반 동기화
CREATE INDEX IF NOT EXISTS idx_ingest_release_date_updated_at_id
    ON ingest.release_date (updated_at, id);

-- platform: 커서 기반 동기화
CREATE INDEX IF NOT EXISTS idx_ingest_platform_updated_at_id
    ON ingest.platform (updated_at, id);

-- company: 커서 기반 동기화
CREATE INDEX IF NOT EXISTS idx_ingest_company_updated_at_id
    ON ingest.company (updated_at, id);

-- involved_company: 커서 기반 동기화
CREATE INDEX IF NOT EXISTS idx_ingest_involved_company_updated_at_id
    ON ingest.involved_company (updated_at, id);

-- language_support: 커서 기반 동기화
CREATE INDEX IF NOT EXISTS idx_ingest_language_support_updated_at_id
    ON ingest.language_support (updated_at, id);

-- game_localization: 커서 기반 동기화
CREATE INDEX IF NOT EXISTS idx_ingest_game_localization_updated_at_id
    ON ingest.game_localization (updated_at, id);
