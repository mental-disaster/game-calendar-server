package com.projectgc.batch.models.dto.igdb

import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy::class)
data class IgdbInvolvedCompanyDto(
    val id: Long,
    val game: Long,
    val company: Long,
    val developer: Boolean?,
    val publisher: Boolean?,
    val porting: Boolean?,
    val supporting: Boolean?,
    val checksum: String?,
    val updatedAt: Long?,
)
