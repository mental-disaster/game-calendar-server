-- game_localization.name, alternative_name.name: NOT NULL → NULL
-- IGDB 데이터에서 해당 필드가 누락된 레코드가 존재하여 파싱 오류 발생
ALTER TABLE ingest.game_localization ALTER COLUMN name DROP NOT NULL;
ALTER TABLE ingest.alternative_name  ALTER COLUMN name DROP NOT NULL;
