package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import com.projectgc.calendar.repository.etl.ServiceEtlSourceLogEntry
import com.projectgc.calendar.repository.etl.ServiceEtlTableSyncResult
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
        private const val SLICE2_PENDING_NOTE =
            "slice2 pending: game-dependent projection materialization is not implemented yet"

        private val DEFERRED_SOURCE_TABLES = listOf(
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

    private val slice2SourceTables = listOf(
        NonCursorSourceTable("game_status", serviceEtlJdbcRepository::syncGameStatuses),
        NonCursorSourceTable("game_type", serviceEtlJdbcRepository::syncGameTypes),
        NonCursorSourceTable("language", serviceEtlJdbcRepository::syncLanguages),
        NonCursorSourceTable("region", serviceEtlJdbcRepository::syncRegions),
        NonCursorSourceTable("release_date_region", serviceEtlJdbcRepository::syncReleaseRegions),
        NonCursorSourceTable("release_date_status", serviceEtlJdbcRepository::syncReleaseStatuses),
        NonCursorSourceTable("genre", serviceEtlJdbcRepository::syncGenres),
        NonCursorSourceTable("theme", serviceEtlJdbcRepository::syncThemes),
        NonCursorSourceTable("player_perspective", serviceEtlJdbcRepository::syncPlayerPerspectives),
        NonCursorSourceTable("game_mode", serviceEtlJdbcRepository::syncGameModes),
        NonCursorSourceTable("keyword", serviceEtlJdbcRepository::syncKeywords),
        NonCursorSourceTable("language_support_type", serviceEtlJdbcRepository::syncLanguageSupportTypes),
        NonCursorSourceTable("website_type", serviceEtlJdbcRepository::syncWebsiteTypes),
        NonCursorSourceTable("platform_logo", serviceEtlJdbcRepository::syncPlatformLogos),
        NonCursorSourceTable("platform_type", serviceEtlJdbcRepository::syncPlatformTypes),
        NonCursorSourceTable("platform", serviceEtlJdbcRepository::syncPlatforms),
        NonCursorSourceTable("company", serviceEtlJdbcRepository::syncCompanies),
    )

    override fun run(runId: UUID, trigger: ServiceEtlTrigger) {
        val startedAt = Instant.now()
        log.info("service ETL 시작 (runId=$runId, trigger=${trigger.type}, ingestSyncId=${trigger.ingestSyncId})")
        serviceEtlJdbcRepository.insertRunLog(runId, trigger, startedAt)

        try {
            transactionTemplate.executeWithoutResult {
                slice2SourceTables.forEach { sourceTable -> syncSourceTable(runId, sourceTable) }
                DEFERRED_SOURCE_TABLES.forEach { tableName -> logSkippedSource(runId, tableName) }
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

    private fun syncSourceTable(runId: UUID, sourceTable: SourceTableDefinition) {
        val tableStartedAt = Instant.now()
        val cursorFrom = if (sourceTable.usesCursor) {
            serviceEtlJdbcRepository.findCursor(sourceTable.tableName)
        } else {
            null
        }
        val result = sourceTable.sync(cursorFrom)
        val tableFinishedAt = Instant.now()

        if (sourceTable.usesCursor && result.nextCursor != null && result.nextCursor != cursorFrom) {
            serviceEtlJdbcRepository.upsertCursor(
                tableName = sourceTable.tableName,
                lastSyncedAt = result.nextCursor,
                syncedAt = tableFinishedAt,
            )
        }

        serviceEtlJdbcRepository.insertSourceLog(
            ServiceEtlSourceLogEntry(
                runId = runId,
                tableName = sourceTable.tableName,
                status = COMPLETED,
                processedRows = result.processedRows,
                cursorFrom = cursorFrom,
                cursorTo = if (sourceTable.usesCursor) result.nextCursor ?: cursorFrom else null,
                note = result.note,
                startedAt = tableStartedAt,
                finishedAt = tableFinishedAt,
            )
        )
    }

    private fun logSkippedSource(runId: UUID, tableName: String) {
        val loggedAt = Instant.now()
        val cursor = serviceEtlJdbcRepository.findCursor(tableName)
        serviceEtlJdbcRepository.insertSourceLog(
            ServiceEtlSourceLogEntry(
                runId = runId,
                tableName = tableName,
                status = SKIPPED,
                processedRows = 0,
                cursorFrom = cursor,
                cursorTo = cursor,
                note = SLICE2_PENDING_NOTE,
                startedAt = loggedAt,
                finishedAt = loggedAt,
            )
        )
    }
}

private sealed interface SourceTableDefinition {
    val tableName: String
    val usesCursor: Boolean

    fun sync(cursor: Long?): ServiceEtlTableSyncResult
}

private data class NonCursorSourceTable(
    override val tableName: String,
    private val syncer: () -> ServiceEtlTableSyncResult,
) : SourceTableDefinition {
    override val usesCursor: Boolean = false

    override fun sync(cursor: Long?): ServiceEtlTableSyncResult = syncer()
}
