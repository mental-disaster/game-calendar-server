package com.projectgc.batch.models.dto.igdb

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbCompanyDto(
    val id: Long,
    val name: String?,
    val parent: Long?,
    val changedCompanyId: Long?,
    val developed: List<Long>?,
    val published: List<Long>?,
    val checksum: String?,
    val updatedAt: Long?,
)
