package com.projectgc.calendar.web

import com.projectgc.calendar.model.GameReleaseSummary
import com.projectgc.calendar.service.GameReleaseService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping

// TODO: service 스키마 구현 완료 후 @RestController 추가
@RequestMapping("/api/releases")
class GameReleaseController(
    private val gameReleaseService: GameReleaseService
) {

    @GetMapping("/upcoming")
    fun upcomingReleases(): List<GameReleaseSummary> {
        return gameReleaseService.findUpcomingReleases()
    }

    @GetMapping("/recent")
    fun recentReleases(): List<GameReleaseSummary> {
        return gameReleaseService.findRecentReleases()
    }
}
