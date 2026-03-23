package com.projectgc.batch.models.dto.igdb

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbReleaseDateDto(
    val id: Long,
    val game: Long,
    val platform: Long?,
    val region: Long?,   // ingest.release_date.release_region 에 저장 (컬럼명 주의)
    val status: Long?,
    val date: Long?,
    val y: Int?,
    val m: Int?,
    val human: String?,
    val checksum: String?,
    val updatedAt: Long?,
)
