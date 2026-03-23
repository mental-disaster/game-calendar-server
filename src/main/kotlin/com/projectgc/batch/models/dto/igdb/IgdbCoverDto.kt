package com.projectgc.batch.models.dto.igdb

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbCoverDto(
    val id: Long,
    val game: Long?,
    val gameLocalization: Long?,
    val imageId: String?,
    val url: String?,
    val checksum: String?,
)
