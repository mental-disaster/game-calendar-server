package com.projectgc.calendar.web

import com.projectgc.calendar.model.GameReleaseSummary
import com.projectgc.calendar.service.GameReleaseService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * HTTP entry point exposing release calendar reads.
 */
@RestController
@RequestMapping("/api/releases")
class GameReleaseController(
    private val gameReleaseService: GameReleaseService
) {

    @GetMapping("/upcoming")
    fun upcomingReleases(): List<GameReleaseSummary> {
        // Returns the planned releases after today.
        return gameReleaseService.findUpcomingReleases()
    }

    @GetMapping("/recent")
    fun recentReleases(): List<GameReleaseSummary> {
        // Returns the titles released recently for recap sections.
        return gameReleaseService.findRecentReleases()
    }
}
