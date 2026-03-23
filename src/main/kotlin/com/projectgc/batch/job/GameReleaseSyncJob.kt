package com.projectgc.batch.job

import com.projectgc.batch.service.GameReleaseBatchService
import org.slf4j.LoggerFactory
import org.springframework.core.task.TaskRejectedException
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class GameReleaseSyncJob(
    private val gameReleaseBatchService: GameReleaseBatchService,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "\${batch.sync.cron}")
    fun runNightlySync() {
        try {
            log.info("야간 IGDB 동기화 잡 시작")
            gameReleaseBatchService.syncAll()
        } catch (e: TaskRejectedException) {
            log.warn("야간 동기화 스킵 — 이전 실행이 아직 진행 중입니다.")
        }
    }
}
