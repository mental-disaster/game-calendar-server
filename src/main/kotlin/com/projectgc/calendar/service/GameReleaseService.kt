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
        // TODO: Load releases after the reference date and map them to summaries.
        throw NotImplementedError("Upcoming release retrieval is not implemented yet.")
    }

    fun findRecentReleases(referenceDate: LocalDate = LocalDate.now()): List<GameReleaseSummary> {
        // TODO: Load releases before the reference date and map them to summaries.
        throw NotImplementedError("Recent release retrieval is not implemented yet.")
    }
}
