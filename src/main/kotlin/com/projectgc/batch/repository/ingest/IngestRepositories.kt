package com.projectgc.batch.repository.ingest

import org.springframework.stereotype.Component

@Component
class IngestRepositories(
    val jdbc: IngestJdbcRepository,
    // 핵심 테이블
    val game: IngestGameRepository,
    val releaseDate: IngestReleaseDateRepository,
    val platform: IngestPlatformRepository,
    val company: IngestCompanyRepository,
    val involvedCompany: IngestInvolvedCompanyRepository,
    val cover: IngestCoverRepository,
    val languageSupport: IngestLanguageSupportRepository,
    val gameLocalization: IngestGameLocalizationRepository,
    // 참조 테이블
    val genre: IngestGenreRepository,
    val theme: IngestThemeRepository,
    val playerPerspective: IngestPlayerPerspectiveRepository,
    val gameMode: IngestGameModeRepository,
    val keyword: IngestKeywordRepository,
    val languageSupportType: IngestLanguageSupportTypeRepository,
    val platformType: IngestPlatformTypeRepository,
    val gameStatus: IngestGameStatusRepository,
    val gameType: IngestGameTypeRepository,
    val websiteType: IngestWebsiteTypeRepository,
    val language: IngestLanguageRepository,
    val region: IngestRegionRepository,
    val releaseDateRegion: IngestReleaseDateRegionRepository,
    val releaseDateStatus: IngestReleaseDateStatusRepository,
    val platformLogo: IngestPlatformLogoRepository,
    // 미디어
    val artwork: IngestArtworkRepository,
    val screenshot: IngestScreenshotRepository,
    val gameVideo: IngestGameVideoRepository,
    val website: IngestWebsiteRepository,
    val alternativeName: IngestAlternativeNameRepository,
    // 커서
    val syncCursor: IngestSyncCursorRepository,
)
