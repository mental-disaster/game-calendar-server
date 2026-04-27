package com.projectgc.batch.models.dto.igdb

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbPlatformDto(
    val id: Long,
    val name: String?,
    val abbreviation: String?,
    val alternativeName: String?,
    val platformLogo: Long?,
    val platformType: Long?,
    val checksum: String?,
    val updatedAt: Long?,
)
