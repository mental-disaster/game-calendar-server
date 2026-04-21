package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import org.springframework.stereotype.Service

@Service
class AffectedGameIdCalculator(
    private val serviceEtlJdbcRepository: ServiceEtlJdbcRepository,
) {
    companion object {
        private const val INITIAL_FULL_SWEEP_NOTE =
            "slice3 initial full sweep: cursor missing, projection materialization is not implemented yet"
        private const val UPDATED_AT_DELTA_NOTE =
            "slice3 affected game_id delta calculated from source updated_at, projection materialization is not implemented yet"
        private const val GAME_UPDATED_STRATEGY_NOTE =
            "slice3 affected game_id delta calculated from ingest.game.updated_at, projection materialization is not implemented yet"
    }

    private val sourceTables = listOf(
        UpdatedAtSourceTable("game", serviceEtlJdbcRepository::findAffectedGameIdsFromGames),
        UpdatedAtSourceTable("release_date", serviceEtlJdbcRepository::findAffectedGameIdsFromReleaseDates),
        UpdatedAtSourceTable("involved_company", serviceEtlJdbcRepository::findAffectedGameIdsFromInvolvedCompanies),
        UpdatedAtSourceTable("language_support", serviceEtlJdbcRepository::findAffectedGameIdsFromLanguageSupports),
        UpdatedAtSourceTable("game_localization", serviceEtlJdbcRepository::findAffectedGameIdsFromGameLocalizations),
        // These sources are fetched in ingest by changed game IDs, so ingest.game.updated_at is the stable delta boundary.
        GameUpdatedSourceTable("cover"),
        GameUpdatedSourceTable("artwork"),
        GameUpdatedSourceTable("screenshot"),
        GameUpdatedSourceTable("game_video"),
        GameUpdatedSourceTable("website"),
        GameUpdatedSourceTable("alternative_name"),
    )

    fun calculate(syncStartedAt: Long): AffectedGameIdCalculationResult {
        val allGameIds by lazy { serviceEtlJdbcRepository.findAllIngestGameIds().toSet() }
        val sourceResults = sourceTables.map { sourceTable ->
            val cursorFrom = serviceEtlJdbcRepository.findCursor(sourceTable.tableName)
            if (cursorFrom == null) {
                AffectedGameIdSourceResult(
                    tableName = sourceTable.tableName,
                    cursorFrom = null,
                    cursorTo = syncStartedAt,
                    affectedGameIds = allGameIds,
                    note = INITIAL_FULL_SWEEP_NOTE,
                )
            } else {
                AffectedGameIdSourceResult(
                    tableName = sourceTable.tableName,
                    cursorFrom = cursorFrom,
                    cursorTo = syncStartedAt,
                    affectedGameIds = sourceTable.collect(cursorFrom),
                    note = sourceTable.note,
                )
            }
        }

        val affectedGameIds = linkedSetOf<Long>()
        sourceResults.forEach { result -> affectedGameIds += result.affectedGameIds }

        return AffectedGameIdCalculationResult(
            affectedGameIds = affectedGameIds,
            sourceResults = sourceResults,
        )
    }

    private sealed interface SourceTableDefinition {
        val tableName: String
        val note: String

        fun collect(cursorFrom: Long): Set<Long>
    }

    private data class UpdatedAtSourceTable(
        override val tableName: String,
        private val collector: (Long) -> Set<Long>,
    ) : SourceTableDefinition {
        override val note: String = UPDATED_AT_DELTA_NOTE

        override fun collect(cursorFrom: Long): Set<Long> = collector(cursorFrom)
    }

    private inner class GameUpdatedSourceTable(
        override val tableName: String,
    ) : SourceTableDefinition {
        override val note: String = GAME_UPDATED_STRATEGY_NOTE

        override fun collect(cursorFrom: Long): Set<Long> =
            serviceEtlJdbcRepository.findAffectedGameIdsFromGameUpdatedAt(cursorFrom)
    }
}

data class AffectedGameIdCalculationResult(
    val affectedGameIds: Set<Long>,
    val sourceResults: List<AffectedGameIdSourceResult>,
)

data class AffectedGameIdSourceResult(
    val tableName: String,
    val cursorFrom: Long?,
    val cursorTo: Long,
    val affectedGameIds: Set<Long>,
    val note: String,
)
