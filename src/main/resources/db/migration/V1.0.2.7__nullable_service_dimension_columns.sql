ALTER TABLE service.game_status ALTER COLUMN status DROP NOT NULL;
ALTER TABLE service.game_type ALTER COLUMN type DROP NOT NULL;

ALTER TABLE service.language ALTER COLUMN locale DROP NOT NULL;
ALTER TABLE service.language ALTER COLUMN name DROP NOT NULL;

ALTER TABLE service.region ALTER COLUMN name DROP NOT NULL;
ALTER TABLE service.release_region ALTER COLUMN name DROP NOT NULL;
ALTER TABLE service.release_status ALTER COLUMN name DROP NOT NULL;

ALTER TABLE service.genre ALTER COLUMN name DROP NOT NULL;
ALTER TABLE service.theme ALTER COLUMN name DROP NOT NULL;
ALTER TABLE service.player_perspective ALTER COLUMN name DROP NOT NULL;
ALTER TABLE service.game_mode ALTER COLUMN name DROP NOT NULL;
ALTER TABLE service.keyword ALTER COLUMN name DROP NOT NULL;
ALTER TABLE service.language_support_type ALTER COLUMN name DROP NOT NULL;
ALTER TABLE service.website_type ALTER COLUMN type DROP NOT NULL;

ALTER TABLE service.platform_logo ALTER COLUMN image_id DROP NOT NULL;
ALTER TABLE service.platform_type ALTER COLUMN name DROP NOT NULL;
ALTER TABLE service.platform ALTER COLUMN name DROP NOT NULL;

ALTER TABLE service.company ALTER COLUMN name DROP NOT NULL;
