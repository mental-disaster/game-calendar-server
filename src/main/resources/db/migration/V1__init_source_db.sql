-- IGDB Game 테이블
DROP TABLE IF EXISTS game RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS game (
    id                   BIGINT NOT NULL,              -- 고유 ID / Integer

    name                 TEXT NOT NULL,                -- 게임명 / String
    alternative_names    BIGINT[] NULL,                -- 대체 이름 ID 배열
    game_localizations   BIGINT[] NULL,                -- 지역/언어 로컬라이제이션 ID 배열 / Array of Game Localization IDs
    slug                 TEXT NOT NULL,                -- URL-safe 고유 슬러그 / String

    first_release_date   BIGINT NULL,                  -- 최초 출시일 / Unix Time Stamp
    release_dates        BIGINT[] NULL,                -- 출시일 레코드 ID 배열 / Array of Release Date IDs
    platforms            BIGINT[] NULL,                -- 출시 플랫폼 ID 배열 / Array of Platform IDs
    game_status          BIGINT NULL,                  -- 게임 출시 상태(ex. 개발중단) ID / Reference ID for Game Status
    game_type            BIGINT NULL,                  -- 게임 타입(ex. 메인 게임, DLC) ID / Reference ID for Game Type
    language_supports    BIGINT[] NULL,                -- 지원 언어/자막/음성 ID 배열 / Array of Language Support IDs

    summary              TEXT NULL,                    -- 게임 설명 / String
    storyline            TEXT NULL,                    -- 스토리라인 요약 / String
    genres               BIGINT[] NULL,                -- 장르 ID 배열 / Array of Genre IDs
    themes               BIGINT[] NULL,                -- 테마 ID 배열 / Array of Theme IDs
    player_perspectives  BIGINT[] NULL,                -- 플레이어 관점(ex. 1인칭, 3인칭) ID 배열 / Array of Player Perspective IDs
    game_modes           BIGINT[] NULL,                -- 게임 모드(ex. 싱글, 멀티) ID 배열 / Array of Game Mode IDs
    keywords             BIGINT[] NULL,                -- 키워드 ID 배열 / Array of Keyword IDs
    involved_companies   BIGINT[] NULL,                -- 참여 회사 ID 배열 / Array of Involved Company IDs

    parent_game          BIGINT NULL,                  -- 부모 게임/번들 ID / Reference ID for Game
    remakes              BIGINT[] NULL,                -- 리메이크 게임 ID 배열
    remasters            BIGINT[] NULL,                -- 리마스터 게임 ID 배열
    ports                BIGINT[] NULL,                -- 포팅판 게임 ID 배열
    standalone_expansions BIGINT[] NULL,               -- 스탠드얼론 확장판 ID 배열
    similar_games        BIGINT[] NULL,                -- 유사 게임 ID 배열

    cover                BIGINT NULL,                  -- 커버 이미지 ID / Reference ID for Cover
    artworks             BIGINT[] NULL,                -- 아트워크 ID 배열 / Array of Artwork IDs
    screenshots          BIGINT[] NULL,                -- 스크린샷 ID 배열 / Array of Screenshot IDs
    videos               BIGINT[] NULL,                -- 비디오 ID 배열 / Array of Game Video IDs
    websites             BIGINT[] NULL,                -- 외부 웹사이트 ID 배열 / Array of Website IDs

    tags                 BIGINT[] NULL,                -- 관련 태그(Theme, Genre, Keyword, Game, Player Perspective) 배열 / Array of Tag Numbers

    checksum             UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at           BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at          TIMESTAMPTZ NOT NULL DEFAULT now() -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE game IS 'IGDB Games - https://api-docs.igdb.com/#game';

COMMENT ON COLUMN game.id                     IS '고유 ID';
COMMENT ON COLUMN game.name                   IS '게임명';
COMMENT ON COLUMN game.alternative_names      IS '대체 이름 ID 배열';
COMMENT ON COLUMN game.game_localizations     IS '지역/언어 로컬라이제이션 ID 배열';
COMMENT ON COLUMN game.slug                   IS 'URL-safe 고유 슬러그';
COMMENT ON COLUMN game.first_release_date     IS '최초 출시일';
COMMENT ON COLUMN game.release_dates          IS '출시일 레코드 ID 배열';
COMMENT ON COLUMN game.platforms              IS '출시 플랫폼 ID 배열';
COMMENT ON COLUMN game.game_status            IS '게임 출시 상태(ex. 개발중단) ID';
COMMENT ON COLUMN game.game_type              IS '게임 타입(ex. 메인 게임, DLC) ID';
COMMENT ON COLUMN game.language_supports      IS '지원 언어/자막/음성 ID 배열';
COMMENT ON COLUMN game.summary                IS '게임 설명';
COMMENT ON COLUMN game.storyline              IS '스토리라인 요약';
COMMENT ON COLUMN game.genres                 IS '장르 ID 배열';
COMMENT ON COLUMN game.themes                 IS '테마 ID 배열';
COMMENT ON COLUMN game.player_perspectives    IS '플레이어 관점(ex. 1인칭, 3인칭) ID 배열';
COMMENT ON COLUMN game.game_modes             IS '게임 모드(ex. 싱글, 멀티) ID 배열';
COMMENT ON COLUMN game.keywords               IS '키워드 ID 배열';
COMMENT ON COLUMN game.involved_companies     IS '참여 회사 ID 배열';
COMMENT ON COLUMN game.parent_game            IS '부모 게임/번들 ID';
COMMENT ON COLUMN game.remakes                IS '리메이크 게임 ID 배열';
COMMENT ON COLUMN game.remasters              IS '리마스터 게임 ID 배열';
COMMENT ON COLUMN game.ports                  IS '포팅판 게임 ID 배열';
COMMENT ON COLUMN game.standalone_expansions  IS '스탠드얼론 확장판 ID 배열';
COMMENT ON COLUMN game.similar_games          IS '유사 게임 ID 배열';
COMMENT ON COLUMN game.cover                  IS '커버 이미지 ID';
COMMENT ON COLUMN game.artworks               IS '아트워크 ID 배열';
COMMENT ON COLUMN game.screenshots            IS '스크린샷 ID 배열';
COMMENT ON COLUMN game.videos                 IS '비디오 ID 배열';
COMMENT ON COLUMN game.websites               IS '외부 웹사이트 ID 배열';
COMMENT ON COLUMN game.tags                   IS '관련 태그(숫자 코드) 배열 (Theme/Genre/Keyword/PP/Game)';
COMMENT ON COLUMN game.checksum               IS '객체 체크섬(내용 변경 감지)';
COMMENT ON COLUMN game.updated_at             IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN game.ingested_at            IS '데이터 적재 시각';



-- IGDB Alternative Name 테이블
DROP TABLE IF EXISTS alternative_name RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS alternative_name (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    name          TEXT NOT NULL,                -- 대체 이름 / String
    comment       TEXT NULL,                    -- 대체 이름의 유형 설명 (예: 약칭, 작업 제목, 일본어 제목 등) / String
    game          BIGINT NOT NULL,              -- 관련 게임 ID / Reference ID for Game

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE alternative_name IS 'IGDB Alternative Names — https://api-docs.igdb.com/#alternative-name';

COMMENT ON COLUMN alternative_name.id          IS '고유 ID';
COMMENT ON COLUMN alternative_name.name        IS '대체 이름';
COMMENT ON COLUMN alternative_name.comment     IS '대체 이름의 유형 설명 (예: 약칭, 작업 제목, 일본어 제목 등)';
COMMENT ON COLUMN alternative_name.game        IS '관련 게임 ID';
COMMENT ON COLUMN alternative_name.checksum    IS '객체 체크섬';
COMMENT ON COLUMN alternative_name.ingested_at IS '데이터 적재 시각';



-- IGDB Game Localization 테이블
DROP TABLE IF EXISTS game_localization RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS game_localization (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    name          TEXT NOT NULL,                -- 로컬라이제이션명 / String
    game          BIGINT NOT NULL,              -- 대상 게임 ID / Reference ID for Game
    region        BIGINT NULL,                  -- 지역 ID / Reference ID for Region
    cover         BIGINT NULL,                  -- 커버 이미지 ID / Reference ID for Cover

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE game_localization IS 'IGDB Game Localizations — https://api-docs.igdb.com/#game-localization';

COMMENT ON COLUMN game_localization.id          IS '고유 ID';
COMMENT ON COLUMN game_localization.name        IS '로컬라이제이션명';
COMMENT ON COLUMN game_localization.game        IS '대상 게임 ID';
COMMENT ON COLUMN game_localization.region      IS '지역 ID';
COMMENT ON COLUMN game_localization.cover       IS '커버 이미지 ID';
COMMENT ON COLUMN game_localization.checksum    IS '객체 체크섬 (변경 감지용)';
COMMENT ON COLUMN game_localization.updated_at  IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN game_localization.ingested_at IS '데이터 적재 시각';



-- IGDB Regions 테이블
DROP TABLE IF EXISTS region RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS region (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    name          TEXT NOT NULL,                -- 지역명 / String
    identifier    TEXT NULL,                    -- 지역 식별자 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE region IS 'IGDB Regions — https://api-docs.igdb.com/#region';

COMMENT ON COLUMN region.id            IS '고유 ID';
COMMENT ON COLUMN region.name          IS '지역명';
COMMENT ON COLUMN region.identifier    IS '지역 식별자';
COMMENT ON COLUMN region.checksum      IS '객체 체크섬';
COMMENT ON COLUMN region.updated_at    IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN region.ingested_at   IS '데이터 적재 시각';



-- IGDB Release Date 테이블
DROP TABLE IF EXISTS release_date RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS release_date (
    id              BIGINT NOT NULL,           -- 고유 ID / Integer

    game            BIGINT NOT NULL,           -- 관련 게임 ID / Reference ID for Game
    platform        BIGINT NULL,               -- 출시 플랫폼 ID / Reference ID for Platform
    release_region  BIGINT NULL,               -- 출시 지역 ID / Reference ID for Release Date Region
    status          BIGINT NULL,               -- 출시 상태(ex. 정식출시, 얼액) ID / Reference ID for Release Date Status

    date            BIGINT NULL,               -- 출시일 / datetime
    y               INTEGER NULL,              -- 연(예: 2018) / Integer
    m               INTEGER NULL,              -- 월(1=January) / Integer
    human           TEXT NULL,                 -- 사람이 읽기 쉬운 날짜 표현 / String

    checksum        UUID NULL,                 -- 객체 체크섬 / uuid
    updated_at      BIGINT NULL,               -- IGDB 최종 갱신 시각 / datetime
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE release_date IS 'IGDB Release Dates — https://api-docs.igdb.com/#release-date';

COMMENT ON COLUMN release_date.id              IS '고유 ID';
COMMENT ON COLUMN release_date.game            IS '관련 게임 ID';
COMMENT ON COLUMN release_date.platform        IS '출시 플랫폼 ID';
COMMENT ON COLUMN release_date.release_region  IS '출시 지역 ID';
COMMENT ON COLUMN release_date.status          IS '출시 상태(ex. 정식출시, 얼액) ID';
COMMENT ON COLUMN release_date.date            IS '출시일';
COMMENT ON COLUMN release_date.y               IS '연';
COMMENT ON COLUMN release_date.m               IS '월(1=January)';
COMMENT ON COLUMN release_date.human           IS '사람이 읽기 쉬운 날짜 표현';
COMMENT ON COLUMN release_date.checksum        IS '객체 체크섬 (변경 감지)';
COMMENT ON COLUMN release_date.updated_at      IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN release_date.ingested_at     IS '데이터 적재 시각';



-- IGDB Release Date Region 테이블
DROP TABLE IF EXISTS release_date_region RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS release_date_region (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    region        TEXT NULL,                    -- 지역명 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE release_date_region IS 'IGDB Regions — https://api-docs.igdb.com/#region';

COMMENT ON COLUMN release_date_region.id          IS '고유 ID';
COMMENT ON COLUMN release_date_region.region      IS '지역명';
COMMENT ON COLUMN release_date_region.checksum    IS '객체 체크섬 (변경 감지용)';
COMMENT ON COLUMN release_date_region.updated_at  IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN release_date_region.ingested_at IS '데이터 적재 시각';



-- IGDB Release Date Status 테이블
DROP TABLE IF EXISTS release_date_status RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS release_date_status (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    name          TEXT NOT NULL,                -- 상태명 / String
    description   TEXT NULL,                    -- 상태 설명 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE release_date_status IS 'IGDB Release Date Status — https://api-docs.igdb.com/#release-date-status';

COMMENT ON COLUMN release_date_status.id            IS '고유 ID';
COMMENT ON COLUMN release_date_status.name          IS '상태명';
COMMENT ON COLUMN release_date_status.description   IS '상태 설명';
COMMENT ON COLUMN release_date_status.checksum      IS '객체 체크섬 (변경 감지용)';
COMMENT ON COLUMN release_date_status.updated_at    IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN release_date_status.ingested_at   IS '데이터 적재 시각';



-- IGDB Platform 테이블
DROP TABLE IF EXISTS platform RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS platform (
    id                BIGINT NOT NULL,              -- 고유 ID / Integer

    name              TEXT NOT NULL,                -- 플랫폼명 / String
    abbreviation      TEXT NULL,                    -- 플랫폼 약칭 / String
    alternative_name  TEXT NULL,                    -- 플랫폼 대체명 / String

    platform_logo     BIGINT NULL,                  -- 플랫폼 로고 ID / Reference ID for Platform Logo
    platform_type     BIGINT NULL,                  -- 플랫폼 타입 ID / Reference ID for Platform Type

    checksum          UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at        BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE platform IS 'IGDB Platforms — https://api-docs.igdb.com/#platform';

COMMENT ON COLUMN platform.id               IS '고유 ID';
COMMENT ON COLUMN platform.name             IS '플랫폼명';
COMMENT ON COLUMN platform.abbreviation     IS '플랫폼 약칭';
COMMENT ON COLUMN platform.alternative_name IS '플랫폼 대체명';
COMMENT ON COLUMN platform.platform_logo    IS '플랫폼 로고 ID';
COMMENT ON COLUMN platform.platform_type    IS '플랫폼 타입 ID';
COMMENT ON COLUMN platform.checksum         IS '객체 체크섬 (변경 감지용)';
COMMENT ON COLUMN platform.updated_at       IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN platform.ingested_at      IS '데이터 적재 시각';



-- IGDB Platform Logo 테이블
DROP TABLE IF EXISTS platform_logo RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS platform_logo (
    id             BIGINT NOT NULL,              -- 고유 ID / Integer

    image_id       TEXT NOT NULL,                -- 이미지 ID (IGDB 이미지 링크 구성용) / String
    url            TEXT NULL,                    -- 이미지 URL / String

    checksum       UUID NULL,                    -- 객체 체크섬 / UUID
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE platform_logo IS 'IGDB Platform Logos — https://api-docs.igdb.com/#platform-logo';

COMMENT ON COLUMN platform_logo.id             IS '고유 ID';
COMMENT ON COLUMN platform_logo.image_id       IS '이미지 ID (IGDB 이미지 링크 구성용)';
COMMENT ON COLUMN platform_logo.url            IS '이미지 URL';
COMMENT ON COLUMN platform_logo.checksum       IS '객체 체크섬 (변경 감지용)';
COMMENT ON COLUMN platform_logo.ingested_at    IS '데이터 적재 시각';



-- IGDB Platform Type 테이블
DROP TABLE IF EXISTS platform_type RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS platform_type (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    name          TEXT NOT NULL,                -- 플랫폼 타입명 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE platform_type IS 'IGDB Platform Types — https://api-docs.igdb.com/#platform-type';

COMMENT ON COLUMN platform_type.id            IS '고유 ID';
COMMENT ON COLUMN platform_type.name          IS '플랫폼 타입명';
COMMENT ON COLUMN platform_type.checksum      IS '객체 체크섬 (변경 감지용)';
COMMENT ON COLUMN platform_type.updated_at    IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN platform_type.ingested_at   IS '데이터 적재 시각';



-- IGDB Game Status 테이블
DROP TABLE IF EXISTS game_status RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS game_status (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    status        TEXT NOT NULL,                -- 게임 상태 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE game_status IS 'IGDB Game Statuses — https://api-docs.igdb.com/#game-status';

COMMENT ON COLUMN game_status.id          IS '고유 ID';
COMMENT ON COLUMN game_status.status      IS '게임 상태';
COMMENT ON COLUMN game_status.checksum    IS '객체 체크섬 (변경 감지용)';
COMMENT ON COLUMN game_status.updated_at  IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN game_status.ingested_at IS '데이터 적재 시각';



-- IGDB Game Type 테이블
DROP TABLE IF EXISTS game_type RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS game_type (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    type          TEXT NOT NULL,                -- 게임 유형 (예: 메인 게임, DLC, 확장팩 등) / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime (Unix Time Stamp)
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE game_type IS 'IGDB Game Types — https://api-docs.igdb.com/#game-type';

COMMENT ON COLUMN game_type.id          IS '고유 ID';
COMMENT ON COLUMN game_type.type        IS '게임 유형 (예: 메인 게임, DLC, 확장팩 등)';
COMMENT ON COLUMN game_type.checksum    IS '객체 체크섬 (변경 감지용)';
COMMENT ON COLUMN game_type.updated_at  IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN game_type.ingested_at IS '데이터 적재 시각';



-- IGDB Language Support 테이블
DROP TABLE IF EXISTS language_support RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS language_support (
    id                      BIGINT NOT NULL,              -- 고유 ID / Integer

    game                    BIGINT NOT NULL,              -- 게임 ID / Reference ID for Game
    language                BIGINT NOT NULL,              -- 언어 ID / Reference ID for Language
    language_support_type   BIGINT NULL,                  -- 언어 지원 유형 ID / Reference ID for Language Support Type

    checksum                UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at              BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE language_support IS 'IGDB Language Supports — https://api-docs.igdb.com/#language-support';

COMMENT ON COLUMN language_support.id                    IS '고유 ID';
COMMENT ON COLUMN language_support.game                  IS '게임 ID';
COMMENT ON COLUMN language_support.language              IS '언어 ID';
COMMENT ON COLUMN language_support.language_support_type IS '언어 지원 유형 ID';
COMMENT ON COLUMN language_support.checksum              IS '객체 체크섬';
COMMENT ON COLUMN language_support.updated_at            IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN language_support.ingested_at           IS '데이터 적재 시각';



-- IGDB Language 테이블
DROP TABLE IF EXISTS language RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS language (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    locale        TEXT NOT NULL,                -- 언어 코드와 국가 코드의 조합 (예: en-US) / String
    name          TEXT NOT NULL,                -- 영어명 / String
    native_name   TEXT NULL,                    -- 자국어명 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE language IS 'IGDB Languages — https://api-docs.igdb.com/#language';

COMMENT ON COLUMN language.id           IS '고유 ID';
COMMENT ON COLUMN language.locale       IS '언어 코드와 국가 코드의 조합 (예: en-US)';
COMMENT ON COLUMN language.name         IS '영어명';
COMMENT ON COLUMN language.native_name  IS '자국어명';
COMMENT ON COLUMN language.checksum     IS '객체 체크섬';
COMMENT ON COLUMN language.updated_at   IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN language.ingested_at  IS '데이터 적재 시각';




-- IGDB Language Support Type 테이블
DROP TABLE IF EXISTS language_support_type RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS language_support_type (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    name          TEXT NOT NULL,                -- 언어 지원 유형명 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE language_support_type IS 'IGDB Language Support Types — https://api-docs.igdb.com/#language-support-type';

COMMENT ON COLUMN language_support_type.id         IS '고유 ID';
COMMENT ON COLUMN language_support_type.name       IS '언어 지원 유형명';
COMMENT ON COLUMN language_support_type.checksum   IS '객체 체크섬';
COMMENT ON COLUMN language_support_type.updated_at IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN language_support_type.ingested_at IS '데이터 적재 시각';



-- IGDB Genre 테이블
DROP TABLE IF EXISTS genre RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS genre (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    name          TEXT NOT NULL,                -- 장르명 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE genre IS 'IGDB Genres — https://api-docs.igdb.com/#genre';

COMMENT ON COLUMN genre.id         IS '고유 ID';
COMMENT ON COLUMN genre.name       IS '장르명';
COMMENT ON COLUMN genre.checksum   IS '객체 체크섬';
COMMENT ON COLUMN genre.updated_at IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN genre.ingested_at IS '데이터 적재 시각';



-- IGDB Theme 테이블
DROP TABLE IF EXISTS theme RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS theme (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    name          TEXT NOT NULL,                -- 테마명 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE theme IS 'IGDB Themes — https://api-docs.igdb.com/#theme';

COMMENT ON COLUMN theme.id         IS '고유 ID';
COMMENT ON COLUMN theme.name       IS '테마명';
COMMENT ON COLUMN theme.checksum   IS '객체 체크섬';
COMMENT ON COLUMN theme.updated_at IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN theme.ingested_at IS '데이터 적재 시각';



-- IGDB Player Perspective 테이블
DROP TABLE IF EXISTS player_perspective RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS player_perspective (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    name          TEXT NOT NULL,                -- 플레이어 관점명 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE player_perspective IS 'IGDB Player Perspectives — https://api-docs.igdb.com/#player-perspective';

COMMENT ON COLUMN player_perspective.id         IS '고유 ID';
COMMENT ON COLUMN player_perspective.name       IS '플레이어 관점명';
COMMENT ON COLUMN player_perspective.checksum   IS '객체 체크섬';
COMMENT ON COLUMN player_perspective.updated_at IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN player_perspective.ingested_at IS '데이터 적재 시각';



-- IGDB Game Mode 테이블
DROP TABLE IF EXISTS game_mode RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS game_mode (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    name          TEXT NOT NULL,                -- 게임 모드명 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE game_mode IS 'IGDB Game Modes — https://api-docs.igdb.com/#game-mode';

COMMENT ON COLUMN game_mode.id         IS '고유 ID';
COMMENT ON COLUMN game_mode.name       IS '게임 모드명';
COMMENT ON COLUMN game_mode.checksum   IS '객체 체크섬';
COMMENT ON COLUMN game_mode.updated_at IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN game_mode.ingested_at IS '데이터 적재 시각';



-- IGDB Keyword 테이블
DROP TABLE IF EXISTS keyword RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS keyword (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    name          TEXT NOT NULL,                -- 키워드명 / String

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE keyword IS 'IGDB Keywords — https://api-docs.igdb.com/#keyword';

COMMENT ON COLUMN keyword.id         IS '고유 ID';
COMMENT ON COLUMN keyword.name       IS '키워드명';
COMMENT ON COLUMN keyword.checksum   IS '객체 체크섬';
COMMENT ON COLUMN keyword.updated_at IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN keyword.ingested_at IS '데이터 적재 시각';



-- IGDB Involved Company 테이블
DROP TABLE IF EXISTS involved_company RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS involved_company (
    id            BIGINT NOT NULL,              -- 고유 ID / Integer

    game          BIGINT NOT NULL,              -- 관련 게임 ID / Reference ID for Game
    company       BIGINT NOT NULL,              -- 관련 회사 ID / Reference ID for Company
    developer     BOOLEAN NULL,                 -- 개발사 여부 / Boolean
    publisher     BOOLEAN NULL,                 -- 퍼블리셔 여부 / Boolean
    porting       BOOLEAN NULL,                 -- 포팅 담당 여부 / Boolean
    supporting    BOOLEAN NULL,                 -- 지원사 여부 / Boolean

    checksum      UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at    BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE involved_company IS 'IGDB Involved Companies — https://api-docs.igdb.com/#involved-company';

COMMENT ON COLUMN involved_company.id          IS '고유 ID';
COMMENT ON COLUMN involved_company.game        IS '관련 게임 ID';
COMMENT ON COLUMN involved_company.company     IS '관련 회사 ID';
COMMENT ON COLUMN involved_company.developer   IS '개발사 여부';
COMMENT ON COLUMN involved_company.publisher   IS '퍼블리셔 여부';
COMMENT ON COLUMN involved_company.porting     IS '포팅 담당 여부';
COMMENT ON COLUMN involved_company.supporting  IS '지원사 여부';
COMMENT ON COLUMN involved_company.checksum    IS '객체 체크섬';
COMMENT ON COLUMN involved_company.updated_at  IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN involved_company.ingested_at IS '데이터 적재 시각';



-- IGDB Company 테이블
DROP TABLE IF EXISTS company RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS company (
    id                    BIGINT NOT NULL,              -- 고유 ID / Integer

    name                  TEXT NOT NULL,                -- 회사명 / String

    parent                BIGINT NULL,                  -- 모회사 ID / Reference ID for Company
    changed_company_id    BIGINT NULL,                  -- 통합/변경된 회사 ID / Reference ID for Company

    developed             BIGINT[] NULL,                -- 개발한 게임 ID 배열 / Array of Game IDs
    published             BIGINT[] NULL,                -- 퍼블리싱한 게임 ID 배열 / Array of Game IDs

    checksum              UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at            BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at           TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE company IS 'IGDB Companies — https://api-docs.igdb.com/#company';

COMMENT ON COLUMN company.id                   IS '고유 ID';
COMMENT ON COLUMN company.name                 IS '회사명';
COMMENT ON COLUMN company.parent               IS '모회사 ID';
COMMENT ON COLUMN company.changed_company_id   IS '통합/변경된 회사 ID';
COMMENT ON COLUMN company.developed            IS '개발한 게임 ID 배열';
COMMENT ON COLUMN company.published            IS '퍼블리싱한 게임 ID 배열';
COMMENT ON COLUMN company.checksum             IS '객체 체크섬';
COMMENT ON COLUMN company.updated_at           IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN company.ingested_at          IS '데이터 적재 시각';



-- IGDB Cover 테이블
DROP TABLE IF EXISTS cover RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS cover (
    id                  BIGINT NOT NULL,              -- 고유 ID / Integer

    game                BIGINT NULL,                  -- 관련 게임 ID / Reference ID for Game
    game_localization   BIGINT NULL,                  -- 관련 게임 로컬라이제이션 ID / Reference ID for Game Localization

    image_id            TEXT NOT NULL,                -- 이미지 식별자 / String
    url                 TEXT NULL,                    -- 이미지 URL / String

    checksum            UUID NULL,                    -- 객체 체크섬 / UUID
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE cover IS 'IGDB Covers — https://api-docs.igdb.com/#cover';

COMMENT ON COLUMN cover.id                 IS '고유 ID';
COMMENT ON COLUMN cover.game               IS '관련 게임 ID';
COMMENT ON COLUMN cover.game_localization  IS '관련 게임 로컬라이제이션 ID';
COMMENT ON COLUMN cover.image_id           IS '이미지 식별자';
COMMENT ON COLUMN cover.url                IS '이미지 URL';
COMMENT ON COLUMN cover.checksum           IS '객체 체크섬';
COMMENT ON COLUMN cover.ingested_at        IS '데이터 적재 시각';



-- IGDB Artwork 테이블
DROP TABLE IF EXISTS artwork RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS artwork (
    id              BIGINT NOT NULL,              -- 고유 ID / Integer

    game            BIGINT NOT NULL,              -- 관련 게임 ID / Reference ID for Game

    image_id        TEXT NOT NULL,                -- 이미지 식별자 / String
    url             TEXT NULL,                    -- 이미지 URL / String

    checksum        UUID NULL,                    -- 객체 체크섬 / UUID
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE artwork IS 'IGDB Artworks — https://api-docs.igdb.com/#artwork';

COMMENT ON COLUMN artwork.id              IS '고유 ID';
COMMENT ON COLUMN artwork.game            IS '관련 게임 ID';
COMMENT ON COLUMN artwork.image_id        IS '이미지 식별자';
COMMENT ON COLUMN artwork.url             IS '이미지 URL';
COMMENT ON COLUMN artwork.checksum        IS '객체 체크섬';
COMMENT ON COLUMN artwork.ingested_at     IS '데이터 적재 시각';



-- IGDB Screenshot 테이블
DROP TABLE IF EXISTS screenshot RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS screenshot (
    id              BIGINT NOT NULL,              -- 고유 ID / Integer

    game            BIGINT NOT NULL,              -- 관련 게임 ID / Reference ID for Game

    image_id        TEXT NOT NULL,                -- 이미지 식별자 / String
    url             TEXT NULL,                    -- 이미지 URL / String

    checksum        UUID NULL,                    -- 객체 체크섬 / UUID
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE screenshot IS 'IGDB Screenshots — https://api-docs.igdb.com/#screenshot';

COMMENT ON COLUMN screenshot.id            IS '고유 ID';
COMMENT ON COLUMN screenshot.game          IS '관련 게임 ID';
COMMENT ON COLUMN screenshot.image_id      IS '이미지 식별자';
COMMENT ON COLUMN screenshot.url           IS '이미지 URL';
COMMENT ON COLUMN screenshot.checksum      IS '객체 체크섬';
COMMENT ON COLUMN screenshot.ingested_at   IS '데이터 적재 시각';



-- IGDB Game Video 테이블
DROP TABLE IF EXISTS game_video RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS game_video (
    id              BIGINT NOT NULL,              -- 고유 ID / Integer

    game            BIGINT NOT NULL,              -- 관련 게임 ID / Reference ID for Game
    name            TEXT NULL,                    -- 비디오 이름 / String
    video_id        TEXT NOT NULL,                -- 외부 비디오 ID(YouTube 등) / String

    checksum        UUID NULL,                    -- 객체 체크섬 / UUID
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE game_video IS 'IGDB Game Videos — https://api-docs.igdb.com/#game-video';

COMMENT ON COLUMN game_video.id          IS '고유 ID';
COMMENT ON COLUMN game_video.game        IS '관련 게임 ID';
COMMENT ON COLUMN game_video.name        IS '비디오 이름';
COMMENT ON COLUMN game_video.video_id    IS '외부 비디오 ID(YouTube 등)';
COMMENT ON COLUMN game_video.checksum    IS '객체 체크섬';
COMMENT ON COLUMN game_video.ingested_at IS '데이터 적재 시각';



-- IGDB Website 테이블
DROP TABLE IF EXISTS website RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS website (
    id              BIGINT NOT NULL,              -- 고유 ID / Integer

    game            BIGINT NOT NULL,              -- 관련 게임 ID / Reference ID for Game
    type            BIGINT NULL,                  -- 웹사이트 타입 ID / Reference ID for Website Type
    url             TEXT NOT NULL,                -- 웹사이트 주소 / String
    trusted         BOOLEAN NULL,                 -- 신뢰된 공식 사이트 여부 / Boolean

    checksum        UUID NULL,                    -- 객체 체크섬 / UUID
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE website IS 'IGDB Websites — https://api-docs.igdb.com/#website';

COMMENT ON COLUMN website.id           IS '고유 ID';
COMMENT ON COLUMN website.game         IS '관련 게임 ID';
COMMENT ON COLUMN website.type         IS '웹사이트 타입 ID';
COMMENT ON COLUMN website.url          IS '웹사이트 주소';
COMMENT ON COLUMN website.trusted      IS '신뢰된 공식 사이트 여부';
COMMENT ON COLUMN website.checksum     IS '객체 체크섬';
COMMENT ON COLUMN website.ingested_at  IS '데이터 적재 시각';



-- IGDB Website Type 테이블
DROP TABLE IF EXISTS website_type RESTART IDENTITY CASCADE;

CREATE TABLE IF NOT EXISTS website_type (
    id              BIGINT NOT NULL,              -- 고유 ID / Integer

    type            TEXT NOT NULL,                -- 웹사이트 타입 / String

    checksum        UUID NULL,                    -- 객체 체크섬 / UUID
    updated_at      BIGINT NULL,                  -- IGDB 최종 갱신 시각 / datetime
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()  -- 데이터 적재 시각 / datetime (system)
);

COMMENT ON TABLE website_type IS 'IGDB Website Types — https://api-docs.igdb.com/#website-type';

COMMENT ON COLUMN website_type.id          IS '고유 ID';
COMMENT ON COLUMN website_type.type        IS '웹사이트 타입';
COMMENT ON COLUMN website_type.checksum    IS '객체 체크섬';
COMMENT ON COLUMN website_type.updated_at  IS 'IGDB 최종 갱신 시각';
COMMENT ON COLUMN website_type.ingested_at IS '데이터 적재 시각';
