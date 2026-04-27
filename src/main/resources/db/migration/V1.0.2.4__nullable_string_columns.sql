-- IGDB API 명세상 id 외 모든 필드는 Optional
-- non-nullable로 선언되어 있던 String 컬럼들을 NULL 허용으로 변경

-- game
ALTER TABLE ingest.game ALTER COLUMN name DROP NOT NULL;
ALTER TABLE ingest.game ALTER COLUMN slug DROP NOT NULL;

-- platform
ALTER TABLE ingest.platform ALTER COLUMN name DROP NOT NULL;

-- company
ALTER TABLE ingest.company ALTER COLUMN name DROP NOT NULL;

-- cover
ALTER TABLE ingest.cover ALTER COLUMN image_id DROP NOT NULL;

-- artwork, screenshot
ALTER TABLE ingest.artwork    ALTER COLUMN image_id DROP NOT NULL;
ALTER TABLE ingest.screenshot ALTER COLUMN image_id DROP NOT NULL;

-- game_video
ALTER TABLE ingest.game_video ALTER COLUMN video_id DROP NOT NULL;

-- website
ALTER TABLE ingest.website ALTER COLUMN url DROP NOT NULL;

-- website_type
ALTER TABLE ingest.website_type ALTER COLUMN type DROP NOT NULL;

-- platform_logo
ALTER TABLE ingest.platform_logo ALTER COLUMN image_id DROP NOT NULL;

-- platform_type
ALTER TABLE ingest.platform_type ALTER COLUMN name DROP NOT NULL;

-- game_status
ALTER TABLE ingest.game_status ALTER COLUMN status DROP NOT NULL;

-- game_type
ALTER TABLE ingest.game_type ALTER COLUMN type DROP NOT NULL;

-- language
ALTER TABLE ingest.language ALTER COLUMN locale DROP NOT NULL;
ALTER TABLE ingest.language ALTER COLUMN name   DROP NOT NULL;

-- language_support_type
ALTER TABLE ingest.language_support_type ALTER COLUMN name DROP NOT NULL;

-- genre, theme, player_perspective, game_mode, keyword
ALTER TABLE ingest.genre              ALTER COLUMN name DROP NOT NULL;
ALTER TABLE ingest.theme              ALTER COLUMN name DROP NOT NULL;
ALTER TABLE ingest.player_perspective ALTER COLUMN name DROP NOT NULL;
ALTER TABLE ingest.game_mode          ALTER COLUMN name DROP NOT NULL;
ALTER TABLE ingest.keyword            ALTER COLUMN name DROP NOT NULL;

-- region
ALTER TABLE ingest.region ALTER COLUMN name DROP NOT NULL;

-- release_date_status
ALTER TABLE ingest.release_date_status ALTER COLUMN name DROP NOT NULL;
