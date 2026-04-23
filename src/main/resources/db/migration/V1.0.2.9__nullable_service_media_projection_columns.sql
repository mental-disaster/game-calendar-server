ALTER TABLE service.alternative_name ALTER COLUMN name DROP NOT NULL;

ALTER TABLE service.cover ALTER COLUMN image_id DROP NOT NULL;
ALTER TABLE service.artwork ALTER COLUMN image_id DROP NOT NULL;
ALTER TABLE service.screenshot ALTER COLUMN image_id DROP NOT NULL;

ALTER TABLE service.game_video ALTER COLUMN video_id DROP NOT NULL;

ALTER TABLE service.website ALTER COLUMN url DROP NOT NULL;
