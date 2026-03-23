package com.projectgc.calendar.service

import com.projectgc.calendar.model.GameReleaseSummary
import java.time.LocalDate
import org.springframework.stereotype.Service

/**
 * Coordinates release lookup rules before exposing them to the web layer.
 */
@Service
class GameReleaseService(
) {
    fun findUpcomingReleases(referenceDate: LocalDate = LocalDate.now()): List<GameReleaseSummary> {
        // TODO: service 스키마 구현 후 연결
        return emptyList()
    }

    fun findRecentReleases(referenceDate: LocalDate = LocalDate.now()): List<GameReleaseSummary> {
        // TODO: service 스키마 구현 후 연결
        return emptyList()
    }
}
