package com.projectgc.batch.models.dto.igdb

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbGameDto(
    val id: Long,
    val name: String?,
    val slug: String?,
    val summary: String?,
    val storyline: String?,
    val firstReleaseDate: Long?,
    val releaseDates: List<Long>?,
    val platforms: List<Long>?,
    val gameStatus: Long?,
    val gameType: Long?,
    val languageSupports: List<Long>?,
    val genres: List<Long>?,
    val themes: List<Long>?,
    val playerPerspectives: List<Long>?,
    val gameModes: List<Long>?,
    val keywords: List<Long>?,
    val involvedCompanies: List<Long>?,
    val parentGame: Long?,
    val remakes: List<Long>?,
    val remasters: List<Long>?,
    val ports: List<Long>?,
    val standaloneExpansions: List<Long>?,
    val similarGames: List<Long>?,
    val cover: Long?,
    val artworks: List<Long>?,
    val screenshots: List<Long>?,
    val videos: List<Long>?,
    val websites: List<Long>?,
    val alternativeNames: List<Long>?,
    val gameLocalizations: List<Long>?,
    val tags: List<Long>?,
    val checksum: String?,
    val updatedAt: Long?,
)
