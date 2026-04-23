package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.GameCompanyProjectionRow
import com.projectgc.calendar.repository.etl.GameDimensionProjectionRow
import com.projectgc.calendar.repository.etl.GameLanguageProjectionRow
import com.projectgc.calendar.repository.etl.GameLocalizationProjectionRow
import com.projectgc.calendar.repository.etl.GameProjectionRow
import com.projectgc.calendar.repository.etl.GameRelationProjectionRow
import com.projectgc.calendar.repository.etl.GameReleaseProjectionRow
import com.projectgc.calendar.repository.etl.IngestEtlReadJdbcRepository
import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import com.projectgc.calendar.repository.etl.ServiceEtlSourceLogEntry
import com.projectgc.calendar.repository.etl.ServiceEtlTableSyncResult
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID

@Service
class ServiceEtlService(
    private val ingestEtlReadJdbcRepository: IngestEtlReadJdbcRepository,
    private val serviceEtlJdbcRepository: ServiceEtlJdbcRepository,
    private val affectedGameIdCalculator: AffectedGameIdCalculator,
    @Qualifier("serviceEtlTransactionTemplate")
    private val transactionTemplate: TransactionTemplate,
) : ServiceEtlRunner {
    private val log = LoggerFactory.getLogger(javaClass)

    companion object {
        private const val COMPLETED = "completed"
        private const val FAILED = "failed"
        private const val SLICE5_GAME_PROJECTION_NOTE =
            "slice5 projections rebuilt: service.game, service.game_release, service.game_localization, service.game_language, service.game_genre, service.game_theme, service.game_player_perspective, service.game_game_mode, service.game_keyword, service.game_company, service.game_relation"
        private const val SLICE5_DEFERRED_SOURCE_NOTE =
            "slice5 deferred source dry-run: cursor remains deferred until slice6 projection materialization"
    }

    private val slice2SourceTables = listOf(
        NonCursorSourceTable("game_status", ingestEtlReadJdbcRepository::loadGameStatuses, serviceEtlJdbcRepository::syncGameStatuses),
        NonCursorSourceTable("game_type", ingestEtlReadJdbcRepository::loadGameTypes, serviceEtlJdbcRepository::syncGameTypes),
        NonCursorSourceTable("language", ingestEtlReadJdbcRepository::loadLanguages, serviceEtlJdbcRepository::syncLanguages),
        NonCursorSourceTable("region", ingestEtlReadJdbcRepository::loadRegions, serviceEtlJdbcRepository::syncRegions),
        NonCursorSourceTable(
            "release_date_region",
            ingestEtlReadJdbcRepository::loadReleaseRegions,
            serviceEtlJdbcRepository::syncReleaseRegions,
        ),
        NonCursorSourceTable(
            "release_date_status",
            ingestEtlReadJdbcRepository::loadReleaseStatuses,
            serviceEtlJdbcRepository::syncReleaseStatuses,
        ),
        NonCursorSourceTable("genre", ingestEtlReadJdbcRepository::loadGenres, serviceEtlJdbcRepository::syncGenres),
        NonCursorSourceTable("theme", ingestEtlReadJdbcRepository::loadThemes, serviceEtlJdbcRepository::syncThemes),
        NonCursorSourceTable(
            "player_perspective",
            ingestEtlReadJdbcRepository::loadPlayerPerspectives,
            serviceEtlJdbcRepository::syncPlayerPerspectives,
        ),
        NonCursorSourceTable("game_mode", ingestEtlReadJdbcRepository::loadGameModes, serviceEtlJdbcRepository::syncGameModes),
        NonCursorSourceTable("keyword", ingestEtlReadJdbcRepository::loadKeywords, serviceEtlJdbcRepository::syncKeywords),
        NonCursorSourceTable(
            "language_support_type",
            ingestEtlReadJdbcRepository::loadLanguageSupportTypes,
            serviceEtlJdbcRepository::syncLanguageSupportTypes,
        ),
        NonCursorSourceTable(
            "website_type",
            ingestEtlReadJdbcRepository::loadWebsiteTypes,
            serviceEtlJdbcRepository::syncWebsiteTypes,
        ),
        NonCursorSourceTable(
            "platform_logo",
            ingestEtlReadJdbcRepository::loadPlatformLogos,
            serviceEtlJdbcRepository::syncPlatformLogos,
        ),
        NonCursorSourceTable(
            "platform_type",
            ingestEtlReadJdbcRepository::loadPlatformTypes,
            serviceEtlJdbcRepository::syncPlatformTypes,
        ),
        NonCursorSourceTable("platform", ingestEtlReadJdbcRepository::loadPlatforms, serviceEtlJdbcRepository::syncPlatforms),
        NonCursorSourceTable("company", ingestEtlReadJdbcRepository::loadCompanies, serviceEtlJdbcRepository::syncCompanies),
    )

    override fun run(runId: UUID, trigger: ServiceEtlTrigger) {
        val startedAt = Instant.now()
        log.info("service ETL 시작 (runId=$runId, trigger=${trigger.type}, ingestSyncId=${trigger.ingestSyncId})")
        serviceEtlJdbcRepository.insertRunLog(runId, trigger, startedAt)

        try {
            val preparedSlice2Sources = slice2SourceTables.map { it.prepare() }
            val preparedAffectedGameInputs = affectedGameIdCalculator.prepare(startedAt.epochSecond)
            var affectedGameCount = 0

            transactionTemplate.executeWithoutResult {
                preparedSlice2Sources.forEach { sourceTable -> syncSourceTable(runId, sourceTable) }
                affectedGameCount = rebuildAffectedGameProjections(runId, preparedAffectedGameInputs)
            }

            serviceEtlJdbcRepository.finishRunLog(
                runId = runId,
                finishedAt = Instant.now(),
                status = COMPLETED,
            )
            log.info("service ETL 완료 (runId=$runId, affectedGames=$affectedGameCount)")
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

    private fun syncSourceTable(runId: UUID, sourceTable: PreparedSourceTableDefinition) {
        val tableStartedAt = Instant.now()
        val result = sourceTable.sync()
        val tableFinishedAt = Instant.now()

        serviceEtlJdbcRepository.insertSourceLog(
            ServiceEtlSourceLogEntry(
                runId = runId,
                tableName = sourceTable.tableName,
                status = COMPLETED,
                processedRows = result.processedRows,
                cursorFrom = null,
                cursorTo = null,
                note = result.note,
                startedAt = tableStartedAt,
                finishedAt = tableFinishedAt,
            )
        )
    }

    private fun rebuildAffectedGameProjections(runId: UUID, preparedInputs: PreparedAffectedGameIdInputs): Int {
        val calculationResult = affectedGameIdCalculator.calculate(preparedInputs)
        val sourceGameRows = preparedInputs.gameRows.filterGameRowsByGameIds(calculationResult.affectedGameIds)
        val materializedGameIds = sourceGameRows.mapTo(linkedSetOf()) { it.id }

        serviceEtlJdbcRepository.rebuildCoreGameProjections(
            gameRows = sourceGameRows,
            gameLocalizationRows = preparedInputs.gameLocalizationRows.filterLocalizationRowsByGameIds(materializedGameIds),
            gameReleaseRows = preparedInputs.gameReleaseRows.filterReleaseRowsByGameIds(materializedGameIds),
        )
        serviceEtlJdbcRepository.rebuildGameDependentBridgeProjections(
            materializedGameIds = materializedGameIds,
            gameLanguageRows = preparedInputs.gameLanguageRows.filterLanguageRowsByGameIds(materializedGameIds),
            gameGenreRows = preparedInputs.gameGenreRows.filterDimensionRowsByGameIds(materializedGameIds),
            gameThemeRows = preparedInputs.gameThemeRows.filterDimensionRowsByGameIds(materializedGameIds),
            gamePlayerPerspectiveRows = preparedInputs.gamePlayerPerspectiveRows.filterDimensionRowsByGameIds(materializedGameIds),
            gameModeRows = preparedInputs.gameModeRows.filterDimensionRowsByGameIds(materializedGameIds),
            gameKeywordRows = preparedInputs.gameKeywordRows.filterDimensionRowsByGameIds(materializedGameIds),
            gameCompanyRows = preparedInputs.gameCompanyRows.filterCompanyRowsByGameIds(materializedGameIds),
            gameRelationRows = preparedInputs.gameRelationRows.filterRelationRowsByGameIds(materializedGameIds),
        )
        calculationResult.sourceResults.forEach { sourceResult ->
            val loggedAt = Instant.now()
            if (sourceResult.advanceCursor && sourceResult.cursorTo != null && sourceResult.cursorTo != sourceResult.cursorFrom) {
                serviceEtlJdbcRepository.upsertCursor(
                    tableName = sourceResult.tableName,
                    lastSyncedAt = sourceResult.cursorTo,
                    syncedAt = loggedAt,
                )
            }
            val sliceNote = if (sourceResult.materializedInCurrentSlice) {
                SLICE5_GAME_PROJECTION_NOTE
            } else {
                SLICE5_DEFERRED_SOURCE_NOTE
            }
            serviceEtlJdbcRepository.insertSourceLog(
                ServiceEtlSourceLogEntry(
                    runId = runId,
                    tableName = sourceResult.tableName,
                    status = COMPLETED,
                    processedRows = sourceResult.affectedGameIds.size,
                    cursorFrom = sourceResult.cursorFrom,
                    cursorTo = sourceResult.cursorTo,
                    note = "${sourceResult.note}; $sliceNote",
                    startedAt = loggedAt,
                    finishedAt = loggedAt,
                )
            )
        }
        return calculationResult.affectedGameIds.size
    }
}

private sealed interface SourceTableDefinition {
    val tableName: String

    fun prepare(): PreparedSourceTableDefinition
}

private sealed interface PreparedSourceTableDefinition {
    val tableName: String

    fun sync(): ServiceEtlTableSyncResult
}

private data class NonCursorSourceTable<T>(
    override val tableName: String,
    private val loader: () -> List<T>,
    private val syncer: (List<T>) -> ServiceEtlTableSyncResult,
) : SourceTableDefinition {
    override fun prepare(): PreparedSourceTableDefinition =
        PreparedNonCursorSourceTable(
            tableName = tableName,
            sourceRows = loader(),
            syncer = syncer,
        )
}

private data class PreparedNonCursorSourceTable<T>(
    override val tableName: String,
    private val sourceRows: List<T>,
    private val syncer: (List<T>) -> ServiceEtlTableSyncResult,
) : PreparedSourceTableDefinition {
    override fun sync(): ServiceEtlTableSyncResult = syncer(sourceRows)
}

private fun List<GameProjectionRow>.filterGameRowsByGameIds(gameIds: Set<Long>): List<GameProjectionRow> =
    filter { it.id in gameIds }

private fun List<GameLocalizationProjectionRow>.filterLocalizationRowsByGameIds(gameIds: Set<Long>): List<GameLocalizationProjectionRow> =
    filter { it.gameId in gameIds }

private fun List<GameReleaseProjectionRow>.filterReleaseRowsByGameIds(gameIds: Set<Long>): List<GameReleaseProjectionRow> =
    filter { it.gameId in gameIds }

private fun List<GameLanguageProjectionRow>.filterLanguageRowsByGameIds(gameIds: Set<Long>): List<GameLanguageProjectionRow> =
    filter { it.gameId in gameIds }

private fun List<GameDimensionProjectionRow>.filterDimensionRowsByGameIds(gameIds: Set<Long>): List<GameDimensionProjectionRow> =
    filter { it.gameId in gameIds }

private fun List<GameCompanyProjectionRow>.filterCompanyRowsByGameIds(gameIds: Set<Long>): List<GameCompanyProjectionRow> =
    filter { it.gameId in gameIds }

private fun List<GameRelationProjectionRow>.filterRelationRowsByGameIds(gameIds: Set<Long>): List<GameRelationProjectionRow> =
    filter { it.gameId in gameIds }
