package com.projectgc.calendar.persistence

import java.time.LocalDate
import org.springframework.data.jpa.repository.JpaRepository

/**
 * Provides queries to load releases within a date window.
 */
interface GameReleaseRepository : JpaRepository<GameReleaseEntity, Long> {
    fun findByReleaseDateBetween(startDate: LocalDate, endDate: LocalDate): List<GameReleaseEntity>
}
