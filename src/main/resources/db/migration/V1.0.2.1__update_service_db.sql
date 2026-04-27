-- service 스키마 업데이트
-- 변경 내용:
--   1. service.game 에 tags 컬럼 추가 + GIN 인덱스
--      - IGDB Tag Number 시스템: (type_id << 28) | object_id 로 인코딩된 배열
--        type: 1=Theme, 2=Genre, 3=Keyword, 4=Player Perspective, 5=Game
--      - 장르/테마/키워드/플레이어관점 등 복합 조건 필터링 시
--        game_genre, game_theme 등 브릿지 테이블 join 없이 단일 GIN 쿼리로 처리 가능
--        예) WHERE tags @> ARRAY[268435469, 536870929]


-- ============================================================================
-- 1. service.game 에 tags 컬럼 추가
-- ============================================================================

ALTER TABLE service.game
    ADD COLUMN IF NOT EXISTS tags BIGINT[];

COMMENT ON COLUMN service.game.tags IS
    'IGDB Tag Number 배열. (type_id << 28) | object_id 인코딩. '
    'type: 1=Theme, 2=Genre, 3=Keyword, 4=Player Perspective, 5=Game. '
    '복합 조건 필터링 시 GIN 인덱스를 통한 고속 검색에 활용';


-- ============================================================================
-- 2. GIN 인덱스 추가
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_game_tags
    ON service.game USING GIN (tags);
