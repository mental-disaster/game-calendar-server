package com.projectgc.calendar.model

import java.time.LocalDate

/**
 * Lightweight representation of an upcoming game release for API responses.
 */
data class GameReleaseSummary(
    val id: Long?,
    val title: String,
    val releaseDate: LocalDate,
    val platform: String?
)
