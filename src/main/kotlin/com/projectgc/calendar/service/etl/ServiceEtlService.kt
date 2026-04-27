package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import com.projectgc.calendar.repository.etl.ServiceEtlSourceLogEntry
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID

@Service
class ServiceEtlService(
    private val serviceEtlJdbcRepository: ServiceEtlJdbcRepository,
    private val transactionTemplate: TransactionTemplate,
) : ServiceEtlRunner {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        private const val COMPLETED = "completed"
        private const val FAILED = "failed"
        private const val SKIPPED = "skipped"
        private const val SLICE1_NOTE = "slice1 skeleton: projection materialization is not implemented yet"

        private val SOURCE_TABLES = listOf(
            "game_status",
            "game_type",
            "language",
            "region",
            "release_date_region",
            "release_date_status",
            "genre",
            "theme",
            "player_perspective",
            "game_mode",
            "keyword",
            "language_support_type",
            "website_type",
            "platform_logo",
            "platform_type",
            "platform",
            "company",
            "game",
            "release_date",
            "involved_company",
            "language_support",
            "game_localization",
            "cover",
            "artwork",
            "screenshot",
            "game_video",
            "website",
            "alternative_name",
        )
    }

    override fun run(runId: UUID, trigger: ServiceEtlTrigger) {
        val startedAt = Instant.now()
        log.info("service ETL 시작 (runId=$runId, trigger=${trigger.type}, ingestSyncId=${trigger.ingestSyncId})")
        serviceEtlJdbcRepository.insertRunLog(runId, trigger, startedAt)

        try {
            transactionTemplate.executeWithoutResult {
                SOURCE_TABLES.forEach { tableName ->
                    val cursor = serviceEtlJdbcRepository.findCursor(tableName)
                    val loggedAt = Instant.now()
                    serviceEtlJdbcRepository.insertSourceLog(
                        ServiceEtlSourceLogEntry(
                            runId = runId,
                            tableName = tableName,
                            status = SKIPPED,
                            processedRows = 0,
                            cursorFrom = cursor,
                            cursorTo = cursor,
                            note = SLICE1_NOTE,
                            startedAt = loggedAt,
                            finishedAt = loggedAt,
                        )
                    )
                }
            }

            serviceEtlJdbcRepository.finishRunLog(
                runId = runId,
                finishedAt = Instant.now(),
                status = COMPLETED,
            )
            log.info("service ETL 완료 (runId=$runId)")
        } catch (ex: Exception) {
            runCatching {
                serviceEtlJdbcRepository.finishRunLog(
                    runId = runId,
                    finishedAt = Instant.now(),
                    status = FAILED,
                    errorMessage = ex.message?.take(1000),
                )
            }.onFailure { finishError ->
                log.error("service ETL 실패 로그 저장 실패 (runId=$runId): ${finishError.message}", finishError)
            }
            log.error("service ETL 실패 (runId=$runId): ${ex.message}", ex)
            throw ex
        }
    }
}
