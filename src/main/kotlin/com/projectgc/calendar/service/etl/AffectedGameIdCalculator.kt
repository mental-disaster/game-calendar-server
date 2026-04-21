package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import org.springframework.stereotype.Service

@Service
class AffectedGameIdCalculator(
    private val serviceEtlJdbcRepository: ServiceEtlJdbcRepository,
) {
    companion object {
        private const val CORE_GAME_DIFF_NOTE =
            "slice4 affected game_id diff calculated from service.game core projection fields"
        private const val GAME_RELEASE_DIFF_NOTE =
            "slice4 affected game_id diff calculated from service.game_release projection"
        private const val GAME_LOCALIZATION_DIFF_NOTE =
            "slice4 affected game_id diff calculated from service.game_localization projection"
        private const val INITIAL_FULL_SWEEP_NOTE =
            "slice4 deferred source dry-run: initial full sweep because cursor is missing"
        private const val UPDATED_AT_DELTA_NOTE =
            "slice4 deferred source dry-run: affected game_id delta calculated from source updated_at"
        private const val GAME_UPDATED_STRATEGY_NOTE =
            "slice4 deferred source dry-run: affected game_id delta calculated from ingest.game.updated_at"
    }

    private val sourceTables = listOf(
        ProjectionDiffSourceTable(
            tableName = "game",
            note = CORE_GAME_DIFF_NOTE,
            collector = serviceEtlJdbcRepository::findAffectedGameIdsFromCoreGameProjectionDiff,
        ),
        ProjectionDiffSourceTable(
            tableName = "release_date",
            note = GAME_RELEASE_DIFF_NOTE,
            collector = serviceEtlJdbcRepository::findAffectedGameIdsFromGameReleaseProjectionDiff,
        ),
        UpdatedAtDryRunSourceTable("involved_company", serviceEtlJdbcRepository::findAffectedGameIdsFromInvolvedCompanies),
        UpdatedAtDryRunSourceTable("language_support", serviceEtlJdbcRepository::findAffectedGameIdsFromLanguageSupports),
        ProjectionDiffSourceTable(
            tableName = "game_localization",
            note = GAME_LOCALIZATION_DIFF_NOTE,
            collector = serviceEtlJdbcRepository::findAffectedGameIdsFromGameLocalizationProjectionDiff,
        ),
        // These sources are fetched in ingest by changed game IDs, so ingest.game.updated_at is the stable delta boundary.
        GameUpdatedDryRunSourceTable("cover"),
        GameUpdatedDryRunSourceTable("artwork"),
        GameUpdatedDryRunSourceTable("screenshot"),
        GameUpdatedDryRunSourceTable("game_video"),
        GameUpdatedDryRunSourceTable("website"),
        GameUpdatedDryRunSourceTable("alternative_name"),
    )

    fun calculate(syncStartedAt: Long): AffectedGameIdCalculationResult {
        val allGameIds by lazy { serviceEtlJdbcRepository.findAllIngestGameIds().toSet() }
        val sourceResults = sourceTables.map { sourceTable -> sourceTable.collect(syncStartedAt) { allGameIds } }

        val affectedGameIds = linkedSetOf<Long>()
        sourceResults
            .filter { result -> result.materializedInCurrentSlice }
            .forEach { result -> affectedGameIds += result.affectedGameIds }

        return AffectedGameIdCalculationResult(
            affectedGameIds = affectedGameIds,
            sourceResults = sourceResults,
        )
    }

    private sealed interface SourceTableDefinition {
        val tableName: String

        fun collect(syncStartedAt: Long, allGameIds: () -> Set<Long>): AffectedGameIdSourceResult
    }

    private data class ProjectionDiffSourceTable(
        override val tableName: String,
        private val note: String,
        private val collector: () -> Set<Long>,
    ) : SourceTableDefinition {
        override fun collect(syncStartedAt: Long, allGameIds: () -> Set<Long>): AffectedGameIdSourceResult =
            AffectedGameIdSourceResult(
                tableName = tableName,
                cursorFrom = null,
                cursorTo = null,
                affectedGameIds = collector(),
                note = note,
                materializedInCurrentSlice = true,
                advanceCursor = false,
            )
    }

    private inner class UpdatedAtDryRunSourceTable(
        override val tableName: String,
        private val collector: (Long) -> Set<Long>,
    ) : SourceTableDefinition {
        override fun collect(syncStartedAt: Long, allGameIds: () -> Set<Long>): AffectedGameIdSourceResult {
            val cursorFrom = serviceEtlJdbcRepository.findCursor(tableName)
            return if (cursorFrom == null) {
                AffectedGameIdSourceResult(
                    tableName = tableName,
                    cursorFrom = null,
                    cursorTo = syncStartedAt,
                    affectedGameIds = allGameIds(),
                    note = INITIAL_FULL_SWEEP_NOTE,
                    materializedInCurrentSlice = false,
                    advanceCursor = false,
                )
            } else {
                AffectedGameIdSourceResult(
                    tableName = tableName,
                    cursorFrom = cursorFrom,
                    cursorTo = syncStartedAt,
                    affectedGameIds = collector(cursorFrom),
                    note = UPDATED_AT_DELTA_NOTE,
                    materializedInCurrentSlice = false,
                    advanceCursor = false,
                )
            }
        }
    }

    private inner class GameUpdatedDryRunSourceTable(
        override val tableName: String,
    ) : SourceTableDefinition {
        override fun collect(syncStartedAt: Long, allGameIds: () -> Set<Long>): AffectedGameIdSourceResult {
            val cursorFrom = serviceEtlJdbcRepository.findCursor(tableName)
            return if (cursorFrom == null) {
                AffectedGameIdSourceResult(
                    tableName = tableName,
                    cursorFrom = null,
                    cursorTo = syncStartedAt,
                    affectedGameIds = allGameIds(),
                    note = INITIAL_FULL_SWEEP_NOTE,
                    materializedInCurrentSlice = false,
                    advanceCursor = false,
                )
            } else {
                AffectedGameIdSourceResult(
                    tableName = tableName,
                    cursorFrom = cursorFrom,
                    cursorTo = syncStartedAt,
                    affectedGameIds = serviceEtlJdbcRepository.findAffectedGameIdsFromGameUpdatedAt(cursorFrom),
                    note = GAME_UPDATED_STRATEGY_NOTE,
                    materializedInCurrentSlice = false,
                    advanceCursor = false,
                )
            }
        }
    }
}

data class AffectedGameIdCalculationResult(
    val affectedGameIds: Set<Long>,
    val sourceResults: List<AffectedGameIdSourceResult>,
)

data class AffectedGameIdSourceResult(
    val tableName: String,
    val cursorFrom: Long?,
    val cursorTo: Long?,
    val affectedGameIds: Set<Long>,
    val note: String,
    val materializedInCurrentSlice: Boolean,
    val advanceCursor: Boolean,
)
