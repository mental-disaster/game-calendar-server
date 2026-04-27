package com.projectgc.batch.models.mapper

import com.projectgc.batch.models.dto.igdb.*
import com.projectgc.batch.models.entity.ingest.*
import org.slf4j.LoggerFactory
import java.util.UUID

private val log = LoggerFactory.getLogger("IngestEntityMappers")

private fun String?.toUuid(): UUID? = this?.let {
    try {
        UUID.fromString(it)
    } catch (e: IllegalArgumentException) {
        log.warn("UUID 파싱 실패, null 처리: '{}'", it)
        null
    }
}
private fun List<Long>?.toArray(): Array<Long>? = this?.toTypedArray()

fun IgdbGameDto.toEntity() = IngestGameEntity().also {
    it.id = id
    it.name = name
    it.slug = slug
    it.summary = summary
    it.storyline = storyline
    it.firstReleaseDate = firstReleaseDate
    it.releaseDates = releaseDates.toArray()
    it.platforms = platforms.toArray()
    it.gameStatus = gameStatus
    it.gameType = gameType
    it.languageSupports = languageSupports.toArray()
    it.genres = genres.toArray()
    it.themes = themes.toArray()
    it.playerPerspectives = playerPerspectives.toArray()
    it.gameModes = gameModes.toArray()
    it.keywords = keywords.toArray()
    it.involvedCompanies = involvedCompanies.toArray()
    it.parentGame = parentGame
    it.remakes = remakes.toArray()
    it.remasters = remasters.toArray()
    it.ports = ports.toArray()
    it.standaloneExpansions = standaloneExpansions.toArray()
    it.similarGames = similarGames.toArray()
    it.cover = cover
    it.artworks = artworks.toArray()
    it.screenshots = screenshots.toArray()
    it.videos = videos.toArray()
    it.websites = websites.toArray()
    it.alternativeNames = alternativeNames.toArray()
    it.gameLocalizations = gameLocalizations.toArray()
    it.tags = tags.toArray()
    it.checksum = checksum.toUuid()
    it.updatedAt = updatedAt
}

fun IgdbReleaseDateDto.toEntity() = IngestReleaseDateEntity().also {
    it.id = id
    it.game = game
    it.platform = platform
    it.releaseRegion = region   // DTO: region → Entity: releaseRegion (컬럼명: release_region)
    it.status = status
    it.date = date
    it.y = y
    it.m = m
    it.human = human
    it.checksum = checksum.toUuid()
    it.updatedAt = updatedAt
}

fun IgdbPlatformDto.toEntity() = IngestPlatformEntity().also {
    it.id = id
    it.name = name
    it.abbreviation = abbreviation
    it.alternativeName = alternativeName
    it.platformLogo = platformLogo
    it.platformType = platformType
    it.checksum = checksum.toUuid()
    it.updatedAt = updatedAt
}

fun IgdbCompanyDto.toEntity() = IngestCompanyEntity().also {
    it.id = id
    it.name = name
    it.parent = parent
    it.changedCompanyId = changedCompanyId
    it.developed = developed.toArray()
    it.published = published.toArray()
    it.checksum = checksum.toUuid()
    it.updatedAt = updatedAt
}

fun IgdbInvolvedCompanyDto.toEntity() = IngestInvolvedCompanyEntity().also {
    it.id = id
    it.game = game
    it.company = company
    it.developer = developer
    it.publisher = publisher
    it.porting = porting
    it.supporting = supporting
    it.checksum = checksum.toUuid()
    it.updatedAt = updatedAt
}

fun IgdbCoverDto.toEntity() = IngestCoverEntity().also {
    it.id = id
    it.game = game
    it.gameLocalization = gameLocalization
    it.imageId = imageId
    it.url = url
    it.checksum = checksum.toUuid()
}

fun IgdbLanguageSupportDto.toEntity() = IngestLanguageSupportEntity().also {
    it.id = id
    it.game = game
    it.language = language
    it.languageSupportType = languageSupportType
    it.checksum = checksum.toUuid()
    it.updatedAt = updatedAt
}

fun IgdbGameLocalizationDto.toEntity() = IngestGameLocalizationEntity().also {
    it.id = id
    it.game = game
    it.region = region
    it.name = name
    it.cover = cover
    it.checksum = checksum.toUuid()
    it.updatedAt = updatedAt
}

// Named entities — IgdbNamedDto 재사용 (엔드포인트별 전용 toXxxEntity 함수)
fun IgdbNamedDto.toGenreEntity() = IngestGenreEntity().also {
    it.id = id; it.name = name; it.checksum = checksum.toUuid(); it.updatedAt = updatedAt
}

fun IgdbNamedDto.toThemeEntity() = IngestThemeEntity().also {
    it.id = id; it.name = name; it.checksum = checksum.toUuid(); it.updatedAt = updatedAt
}

fun IgdbNamedDto.toPlayerPerspectiveEntity() = IngestPlayerPerspectiveEntity().also {
    it.id = id; it.name = name; it.checksum = checksum.toUuid(); it.updatedAt = updatedAt
}

fun IgdbNamedDto.toGameModeEntity() = IngestGameModeEntity().also {
    it.id = id; it.name = name; it.checksum = checksum.toUuid(); it.updatedAt = updatedAt
}

fun IgdbNamedDto.toKeywordEntity() = IngestKeywordEntity().also {
    it.id = id; it.name = name; it.checksum = checksum.toUuid(); it.updatedAt = updatedAt
}

fun IgdbNamedDto.toLanguageSupportTypeEntity() = IngestLanguageSupportTypeEntity().also {
    it.id = id; it.name = name; it.checksum = checksum.toUuid(); it.updatedAt = updatedAt
}

fun IgdbNamedDto.toPlatformTypeEntity() = IngestPlatformTypeEntity().also {
    it.id = id; it.name = name; it.checksum = checksum.toUuid(); it.updatedAt = updatedAt
}

fun IgdbGameStatusDto.toEntity() = IngestGameStatusEntity().also {
    it.id = id; it.status = status; it.checksum = checksum.toUuid(); it.updatedAt = updatedAt
}

fun IgdbGameTypeDto.toEntity() = IngestGameTypeEntity().also {
    it.id = id; it.type = type; it.checksum = checksum.toUuid(); it.updatedAt = updatedAt
}

fun IgdbWebsiteTypeDto.toEntity() = IngestWebsiteTypeEntity().also {
    it.id = id; it.type = type; it.checksum = checksum.toUuid(); it.updatedAt = updatedAt
}

fun IgdbLanguageDto.toEntity() = IngestLanguageEntity().also {
    it.id = id
    it.locale = locale
    it.name = name
    it.nativeName = nativeName
    it.checksum = checksum.toUuid()
    it.updatedAt = updatedAt
}

fun IgdbRegionDto.toEntity() = IngestRegionEntity().also {
    it.id = id
    it.name = name
    it.identifier = identifier
    it.checksum = checksum.toUuid()
    it.updatedAt = updatedAt
}

fun IgdbReleaseDateRegionDto.toEntity() = IngestReleaseDateRegionEntity().also {
    it.id = id; it.region = region; it.checksum = checksum.toUuid(); it.updatedAt = updatedAt
}

fun IgdbReleaseDateStatusDto.toEntity() = IngestReleaseDateStatusEntity().also {
    it.id = id
    it.name = name
    it.description = description
    it.checksum = checksum.toUuid()
    it.updatedAt = updatedAt
}

fun IgdbPlatformLogoDto.toEntity() = IngestPlatformLogoEntity().also {
    it.id = id; it.imageId = imageId; it.url = url; it.checksum = checksum.toUuid()
}

// Media entities — IgdbImageDto 재사용 (artwork / screenshot 구분)
fun IgdbImageDto.toArtworkEntity() = IngestArtworkEntity().also {
    it.id = id; it.game = game; it.imageId = imageId; it.url = url; it.checksum = checksum.toUuid()
}

fun IgdbImageDto.toScreenshotEntity() = IngestScreenshotEntity().also {
    it.id = id; it.game = game; it.imageId = imageId; it.url = url; it.checksum = checksum.toUuid()
}

fun IgdbGameVideoDto.toEntity() = IngestGameVideoEntity().also {
    it.id = id; it.game = game; it.name = name; it.videoId = videoId; it.checksum = checksum.toUuid()
}

fun IgdbWebsiteDto.toEntity() = IngestWebsiteEntity().also {
    it.id = id; it.game = game; it.type = type; it.url = url; it.trusted = trusted; it.checksum = checksum.toUuid()
}

fun IgdbAlternativeNameDto.toEntity() = IngestAlternativeNameEntity().also {
    it.id = id; it.game = game; it.name = name; it.comment = comment; it.checksum = checksum.toUuid()
}
