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
import com.projectgc.calendar.repository.etl.resolveGameCompanyReferences
import com.projectgc.calendar.repository.etl.resolveGameDimensionReferences
import com.projectgc.calendar.repository.etl.resolveGameLanguageReferences
import com.projectgc.calendar.repository.etl.resolveGameLocalizationReferences
import com.projectgc.calendar.repository.etl.resolveGameReferences
import com.projectgc.calendar.repository.etl.resolveGameRelationReferences
import com.projectgc.calendar.repository.etl.resolveGameReleaseReferences
import org.springframework.stereotype.Service

@Service
class AffectedGameIdCalculator(
    private val ingestEtlReadJdbcRepository: IngestEtlReadJdbcRepository,
    private val serviceEtlJdbcRepository: ServiceEtlJdbcRepository,
) {
    companion object {
        private const val GAME_PROJECTION_DIFF_NOTE =
            "slice5 affected game_id diff calculated from service.game core and bridge projections"
        private const val GAME_RELEASE_DIFF_NOTE =
            "slice5 affected game_id diff calculated from service.game_release projection"
        private const val INVOLVED_COMPANY_DIFF_NOTE =
            "slice5 affected game_id diff calculated from service.game_company projection"
        private const val LANGUAGE_SUPPORT_DIFF_NOTE =
            "slice5 affected game_id diff calculated from service.game_language projection"
        private const val GAME_LOCALIZATION_DIFF_NOTE =
            "slice5 affected game_id diff calculated from service.game_localization projection"
        private const val INITIAL_FULL_SWEEP_NOTE =
            "slice5 deferred source dry-run: initial full sweep because cursor is missing"
        private const val UPDATED_AT_DELTA_NOTE =
            "slice5 deferred source dry-run: affected game_id delta calculated from source updated_at"
        private const val GAME_UPDATED_STRATEGY_NOTE =
            "slice5 deferred source dry-run: affected game_id delta calculated from ingest.game.updated_at"
    }

    fun prepare(syncStartedAt: Long): PreparedAffectedGameIdInputs {
        val allGameIds = ingestEtlReadJdbcRepository.findAllIngestGameIds().toSet()
        return PreparedAffectedGameIdInputs(
            allGameIds = allGameIds,
            gameRows = ingestEtlReadJdbcRepository.loadAllGameProjectionRows(),
            gameReleaseRows = ingestEtlReadJdbcRepository.loadAllGameReleaseProjectionRows(),
            gameLocalizationRows = ingestEtlReadJdbcRepository.loadAllGameLocalizationProjectionRows(),
            gameLanguageRows = ingestEtlReadJdbcRepository.loadAllGameLanguageProjectionRows(),
            gameGenreRows = ingestEtlReadJdbcRepository.loadAllGameArrayProjectionRows("genres"),
            gameThemeRows = ingestEtlReadJdbcRepository.loadAllGameArrayProjectionRows("themes"),
            gamePlayerPerspectiveRows = ingestEtlReadJdbcRepository.loadAllGameArrayProjectionRows("player_perspectives"),
            gameModeRows = ingestEtlReadJdbcRepository.loadAllGameArrayProjectionRows("game_modes"),
            gameKeywordRows = ingestEtlReadJdbcRepository.loadAllGameArrayProjectionRows("keywords"),
            gameCompanyRows = ingestEtlReadJdbcRepository.loadAllGameCompanyProjectionRows(),
            gameRelationRows = ingestEtlReadJdbcRepository.loadAllGameRelationProjectionRows(),
            deferredSourceResults = listOf(
                collectGameUpdatedDryRunSource("cover", syncStartedAt, allGameIds),
                collectGameUpdatedDryRunSource("artwork", syncStartedAt, allGameIds),
                collectGameUpdatedDryRunSource("screenshot", syncStartedAt, allGameIds),
                collectGameUpdatedDryRunSource("game_video", syncStartedAt, allGameIds),
                collectGameUpdatedDryRunSource("website", syncStartedAt, allGameIds),
                collectGameUpdatedDryRunSource("alternative_name", syncStartedAt, allGameIds),
            ),
        )
    }

    fun calculate(syncStartedAt: Long): AffectedGameIdCalculationResult = calculate(prepare(syncStartedAt))

    fun calculate(preparedInputs: PreparedAffectedGameIdInputs): AffectedGameIdCalculationResult {
        val allGameIds = preparedInputs.allGameIds
        val sourceResults = listOf(
            projectionDiffResult(
                tableName = "game",
                note = GAME_PROJECTION_DIFF_NOTE,
                affectedGameIds = findAffectedGameIdsFromGameProjectionDiff(preparedInputs),
            ),
            projectionDiffResult(
                tableName = "release_date",
                note = GAME_RELEASE_DIFF_NOTE,
                affectedGameIds = findAffectedGameIdsFromGameReleaseProjectionDiff(
                    ingestGameIds = allGameIds,
                    releaseRows = preparedInputs.gameReleaseRows,
                ),
            ),
            projectionDiffResult(
                tableName = "involved_company",
                note = INVOLVED_COMPANY_DIFF_NOTE,
                affectedGameIds = findAffectedGameIdsFromInvolvedCompanyProjectionDiff(
                    ingestGameIds = allGameIds,
                    companyRows = preparedInputs.gameCompanyRows,
                ),
            ),
            projectionDiffResult(
                tableName = "language_support",
                note = LANGUAGE_SUPPORT_DIFF_NOTE,
                affectedGameIds = findAffectedGameIdsFromLanguageSupportProjectionDiff(
                    ingestGameIds = allGameIds,
                    languageRows = preparedInputs.gameLanguageRows,
                ),
            ),
            projectionDiffResult(
                tableName = "game_localization",
                note = GAME_LOCALIZATION_DIFF_NOTE,
                affectedGameIds = findAffectedGameIdsFromGameLocalizationProjectionDiff(
                    ingestGameIds = allGameIds,
                    localizationRows = preparedInputs.gameLocalizationRows,
                ),
            ),
        ) + preparedInputs.deferredSourceResults

        val affectedGameIds = linkedSetOf<Long>()
        sourceResults
            .filter { it.materializedInCurrentSlice }
            .forEach { affectedGameIds += it.affectedGameIds }

        return AffectedGameIdCalculationResult(
            affectedGameIds = affectedGameIds,
            sourceResults = sourceResults,
        )
    }

    private fun projectionDiffResult(
        tableName: String,
        note: String,
        affectedGameIds: Set<Long>,
    ) = AffectedGameIdSourceResult(
        tableName = tableName,
        cursorFrom = null,
        cursorTo = null,
        affectedGameIds = affectedGameIds,
        note = note,
        materializedInCurrentSlice = true,
        advanceCursor = false,
    )

    private fun collectGameUpdatedDryRunSource(
        tableName: String,
        syncStartedAt: Long,
        allGameIds: Set<Long>,
    ): AffectedGameIdSourceResult {
        val cursorFrom = serviceEtlJdbcRepository.findCursor(tableName)
        return if (cursorFrom == null) {
            AffectedGameIdSourceResult(
                tableName = tableName,
                cursorFrom = null,
                cursorTo = syncStartedAt,
                affectedGameIds = allGameIds,
                note = INITIAL_FULL_SWEEP_NOTE,
                materializedInCurrentSlice = false,
                advanceCursor = false,
            )
        } else {
            AffectedGameIdSourceResult(
                tableName = tableName,
                cursorFrom = cursorFrom,
                cursorTo = syncStartedAt,
                affectedGameIds = ingestEtlReadJdbcRepository.findAffectedGameIdsFromGameUpdatedAt(cursorFrom),
                note = GAME_UPDATED_STRATEGY_NOTE,
                materializedInCurrentSlice = false,
                advanceCursor = false,
            )
        }
    }

    private fun findAffectedGameIdsFromGameProjectionDiff(preparedInputs: PreparedAffectedGameIdInputs): Set<Long> =
        linkedSetOf<Long>().apply {
            addAll(findAffectedGameIdsFromCoreGameProjectionDiff(preparedInputs.gameRows))
            addAll(findAffectedGameIdsFromGameBridgeProjectionDiff(preparedInputs))
        }

    private fun findAffectedGameIdsFromCoreGameProjectionDiff(gameRows: List<GameProjectionRow>): Set<Long> {
        val expectedRows = resolveGameReferences(
            rows = gameRows,
            availableStatusIds = serviceEtlJdbcRepository.loadIds("service.game_status"),
            availableTypeIds = serviceEtlJdbcRepository.loadIds("service.game_type"),
        )
        val actualById = serviceEtlJdbcRepository.loadCurrentGameProjectionRows().associateBy { it.id }
        return expectedRows
            .filter { actualById[it.id] != it }
            .mapTo(linkedSetOf()) { it.id }
    }

    private fun findAffectedGameIdsFromGameBridgeProjectionDiff(preparedInputs: PreparedAffectedGameIdInputs): Set<Long> =
        linkedSetOf<Long>().apply {
            addAll(
                findAffectedGameIdsFromGameArrayProjectionDiff(
                    ingestGameIds = preparedInputs.allGameIds,
                    expectedRows = preparedInputs.gameGenreRows,
                    dimensionTable = "genre",
                    targetTable = "game_genre",
                    targetColumn = "genre_id",
                )
            )
            addAll(
                findAffectedGameIdsFromGameArrayProjectionDiff(
                    ingestGameIds = preparedInputs.allGameIds,
                    expectedRows = preparedInputs.gameThemeRows,
                    dimensionTable = "theme",
                    targetTable = "game_theme",
                    targetColumn = "theme_id",
                )
            )
            addAll(
                findAffectedGameIdsFromGameArrayProjectionDiff(
                    ingestGameIds = preparedInputs.allGameIds,
                    expectedRows = preparedInputs.gamePlayerPerspectiveRows,
                    dimensionTable = "player_perspective",
                    targetTable = "game_player_perspective",
                    targetColumn = "player_perspective_id",
                )
            )
            addAll(
                findAffectedGameIdsFromGameArrayProjectionDiff(
                    ingestGameIds = preparedInputs.allGameIds,
                    expectedRows = preparedInputs.gameModeRows,
                    dimensionTable = "game_mode",
                    targetTable = "game_game_mode",
                    targetColumn = "game_mode_id",
                )
            )
            addAll(
                findAffectedGameIdsFromGameArrayProjectionDiff(
                    ingestGameIds = preparedInputs.allGameIds,
                    expectedRows = preparedInputs.gameKeywordRows,
                    dimensionTable = "keyword",
                    targetTable = "game_keyword",
                    targetColumn = "keyword_id",
                )
            )
            addAll(
                findAffectedGameIdsFromGameRelationProjectionDiff(
                    ingestGameIds = preparedInputs.allGameIds,
                    relationRows = preparedInputs.gameRelationRows,
                )
            )
        }

    private fun findAffectedGameIdsFromGameArrayProjectionDiff(
        ingestGameIds: Set<Long>,
        expectedRows: List<GameDimensionProjectionRow>,
        dimensionTable: String,
        targetTable: String,
        targetColumn: String,
    ): Set<Long> {
        val resolvedRows = resolveGameDimensionReferences(
            rows = expectedRows,
            availableGameIds = ingestGameIds,
            availableDimensionIds = serviceEtlJdbcRepository.loadIds("service.$dimensionTable"),
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentGameDimensionProjectionRows(
            tableName = targetTable,
            targetColumn = targetColumn,
        )
        return findAffectedGameIdsByKey(
            expectedRows = resolvedRows,
            actualRows = actualRows,
            keySelector = { it.gameId to it.dimensionId },
            gameIdSelector = { it.gameId },
            includeActualGameId = { it in ingestGameIds },
        )
    }

    private fun findAffectedGameIdsFromGameRelationProjectionDiff(
        ingestGameIds: Set<Long>,
        relationRows: List<GameRelationProjectionRow>,
    ): Set<Long> {
        val expectedRows = resolveGameRelationReferences(
            rows = relationRows,
            availableGameIds = ingestGameIds,
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentGameRelationProjectionRows()
        return findAffectedGameIdsByKey(
            expectedRows = expectedRows,
            actualRows = actualRows,
            keySelector = { Triple(it.gameId, it.relatedGameId, it.relationType) },
            gameIdSelector = { it.gameId },
            includeActualGameId = { it in ingestGameIds },
        )
    }

    private fun findAffectedGameIdsFromGameReleaseProjectionDiff(
        ingestGameIds: Set<Long>,
        releaseRows: List<GameReleaseProjectionRow>,
    ): Set<Long> {
        val expectedRows = resolveGameReleaseReferences(
            rows = releaseRows,
            availableGameIds = ingestGameIds,
            availablePlatformIds = serviceEtlJdbcRepository.loadIds("service.platform"),
            availableRegionIds = serviceEtlJdbcRepository.loadIds("service.release_region"),
            availableStatusIds = serviceEtlJdbcRepository.loadIds("service.release_status"),
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentGameReleaseProjectionRows()
        return findAffectedGameIdsByKey(
            expectedRows = expectedRows,
            actualRows = actualRows,
            keySelector = { it.id },
            gameIdSelector = { it.gameId },
            includeActualGameId = { it in ingestGameIds },
        )
    }

    private fun findAffectedGameIdsFromInvolvedCompanyProjectionDiff(
        ingestGameIds: Set<Long>,
        companyRows: List<GameCompanyProjectionRow>,
    ): Set<Long> {
        val expectedRows = resolveGameCompanyReferences(
            rows = companyRows,
            availableGameIds = ingestGameIds,
            availableCompanyIds = serviceEtlJdbcRepository.loadIds("service.company"),
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentGameCompanyProjectionRows()
        return findAffectedGameIdsByKey(
            expectedRows = expectedRows,
            actualRows = actualRows,
            keySelector = { it.gameId to it.companyId },
            gameIdSelector = { it.gameId },
            includeActualGameId = { it in ingestGameIds },
        )
    }

    private fun findAffectedGameIdsFromLanguageSupportProjectionDiff(
        ingestGameIds: Set<Long>,
        languageRows: List<GameLanguageProjectionRow>,
    ): Set<Long> {
        val expectedRows = resolveGameLanguageReferences(
            rows = languageRows,
            availableGameIds = ingestGameIds,
            availableLanguageIds = serviceEtlJdbcRepository.loadIds("service.language"),
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentGameLanguageProjectionRows()
        return findAffectedGameIdsByKey(
            expectedRows = expectedRows,
            actualRows = actualRows,
            keySelector = { it.gameId to it.languageId },
            gameIdSelector = { it.gameId },
            includeActualGameId = { it in ingestGameIds },
        )
    }

    private fun findAffectedGameIdsFromGameLocalizationProjectionDiff(
        ingestGameIds: Set<Long>,
        localizationRows: List<GameLocalizationProjectionRow>,
    ): Set<Long> {
        val expectedRows = resolveGameLocalizationReferences(
            rows = localizationRows,
            availableGameIds = ingestGameIds,
            availableRegionIds = serviceEtlJdbcRepository.loadIds("service.region"),
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentGameLocalizationProjectionRows()
        return findAffectedGameIdsByKey(
            expectedRows = expectedRows,
            actualRows = actualRows,
            keySelector = { it.id },
            gameIdSelector = { it.gameId },
            includeActualGameId = { it in ingestGameIds },
        )
    }

    private fun <T, K> findAffectedGameIdsByKey(
        expectedRows: List<T>,
        actualRows: List<T>,
        keySelector: (T) -> K,
        gameIdSelector: (T) -> Long,
        includeActualGameId: (Long) -> Boolean = { true },
    ): Set<Long> {
        val expectedByKey = expectedRows.associateBy(keySelector)
        val actualByKey = actualRows.associateBy(keySelector)
        val affectedGameIds = linkedSetOf<Long>()

        expectedRows.forEach { expectedRow ->
            val key = keySelector(expectedRow)
            val actualRow = actualByKey[key]
            if (actualRow != expectedRow) {
                affectedGameIds += gameIdSelector(expectedRow)
                if (actualRow != null) {
                    val actualGameId = gameIdSelector(actualRow)
                    if (includeActualGameId(actualGameId)) {
                        affectedGameIds += actualGameId
                    }
                }
            }
        }
        actualRows.forEach { actualRow ->
            val key = keySelector(actualRow)
            val gameId = gameIdSelector(actualRow)
            if (!expectedByKey.containsKey(key) && includeActualGameId(gameId)) {
                affectedGameIds += gameId
            }
        }

        return affectedGameIds
    }
}

data class PreparedAffectedGameIdInputs(
    val allGameIds: Set<Long>,
    val gameRows: List<GameProjectionRow>,
    val gameReleaseRows: List<GameReleaseProjectionRow>,
    val gameLocalizationRows: List<GameLocalizationProjectionRow>,
    val gameLanguageRows: List<GameLanguageProjectionRow>,
    val gameGenreRows: List<GameDimensionProjectionRow>,
    val gameThemeRows: List<GameDimensionProjectionRow>,
    val gamePlayerPerspectiveRows: List<GameDimensionProjectionRow>,
    val gameModeRows: List<GameDimensionProjectionRow>,
    val gameKeywordRows: List<GameDimensionProjectionRow>,
    val gameCompanyRows: List<GameCompanyProjectionRow>,
    val gameRelationRows: List<GameRelationProjectionRow>,
    val deferredSourceResults: List<AffectedGameIdSourceResult>,
)

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
