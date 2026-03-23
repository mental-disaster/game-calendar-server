package com.projectgc.batch.models.dto.igdb

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

// id + name 구조의 단순 참조 테이블 DTO
// 사용처: genre, theme, player_perspective, game_mode, keyword,
//         language_support_type, platform_type, website_type
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbNamedDto(
    val id: Long,
    val name: String?,
    val checksum: String?,
    val updatedAt: Long?,
)

// game_status: name 대신 status 필드 사용
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbGameStatusDto(
    val id: Long,
    val status: String?,
    val checksum: String?,
    val updatedAt: Long?,
)

// game_type: name 대신 type 필드 사용
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbGameTypeDto(
    val id: Long,
    val type: String?,
    val checksum: String?,
    val updatedAt: Long?,
)

// release_date_region: name 대신 region 필드 사용
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbReleaseDateRegionDto(
    val id: Long,
    val region: String?,
    val checksum: String?,
    val updatedAt: Long?,
)

// release_date_status: name + description
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbReleaseDateStatusDto(
    val id: Long,
    val name: String?,
    val description: String?,
    val checksum: String?,
    val updatedAt: Long?,
)

// language: locale + name + native_name
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbLanguageDto(
    val id: Long,
    val locale: String?,
    val name: String?,
    val nativeName: String?,
    val checksum: String?,
    val updatedAt: Long?,
)

// language_support: game + language + language_support_type
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbLanguageSupportDto(
    val id: Long,
    val game: Long,
    val language: Long,
    val languageSupportType: Long?,
    val checksum: String?,
    val updatedAt: Long?,
)

// platform_logo: image_id + url
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbPlatformLogoDto(
    val id: Long,
    val imageId: String?,
    val url: String?,
    val checksum: String?,
)

// website_type: id + type
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbWebsiteTypeDto(
    val id: Long,
    val type: String?,
    val checksum: String?,
    val updatedAt: Long?,
)

// artwork, screenshot: id + game + image_id + url
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbImageDto(
    val id: Long,
    val game: Long,
    val imageId: String?,
    val url: String?,
    val checksum: String?,
)

// game_video: id + game + name + video_id
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbGameVideoDto(
    val id: Long,
    val game: Long,
    val name: String?,
    val videoId: String?,
    val checksum: String?,
)

// website: id + game + type + url + trusted
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbWebsiteDto(
    val id: Long,
    val game: Long,
    val type: Long?,
    val url: String?,
    val trusted: Boolean?,
    val checksum: String?,
)

// alternative_name: id + game + name + comment
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbAlternativeNameDto(
    val id: Long,
    val game: Long,
    val name: String?,
    val comment: String?,
    val checksum: String?,
)

// game_localization: id + game + region + name
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbGameLocalizationDto(
    val id: Long,
    val game: Long,
    val region: Long?,
    val name: String?,
    val cover: Long?,
    val checksum: String?,
    val updatedAt: Long?,
)

// region: id + name + identifier
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbRegionDto(
    val id: Long,
    val name: String?,
    val identifier: String?,
    val checksum: String?,
    val updatedAt: Long?,
)
