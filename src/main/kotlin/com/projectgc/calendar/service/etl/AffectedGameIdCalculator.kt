package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.AlternativeNameProjectionRow
import com.projectgc.calendar.repository.etl.ArtworkProjectionRow
import com.projectgc.calendar.repository.etl.CoverProjectionRow
import com.projectgc.calendar.repository.etl.GameCompanyProjectionRow
import com.projectgc.calendar.repository.etl.GameVideoProjectionRow
import com.projectgc.calendar.repository.etl.GameDimensionProjectionRow
import com.projectgc.calendar.repository.etl.GameLanguageProjectionRow
import com.projectgc.calendar.repository.etl.GameLocalizationProjectionRow
import com.projectgc.calendar.repository.etl.GameProjectionRow
import com.projectgc.calendar.repository.etl.GameRelationProjectionRow
import com.projectgc.calendar.repository.etl.GameReleaseProjectionRow
import com.projectgc.calendar.repository.etl.IngestEtlReadJdbcRepository
import com.projectgc.calendar.repository.etl.ScreenshotProjectionRow
import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import com.projectgc.calendar.repository.etl.WebsiteProjectionRow
import com.projectgc.calendar.repository.etl.resolveAlternativeNameReferences
import com.projectgc.calendar.repository.etl.resolveArtworkReferences
import com.projectgc.calendar.repository.etl.resolveCoverReferences
import com.projectgc.calendar.repository.etl.resolveGameCompanyReferences
import com.projectgc.calendar.repository.etl.resolveGameDimensionReferences
import com.projectgc.calendar.repository.etl.resolveGameLanguageReferences
import com.projectgc.calendar.repository.etl.resolveGameLocalizationReferences
import com.projectgc.calendar.repository.etl.resolveGameReferences
import com.projectgc.calendar.repository.etl.resolveGameRelationReferences
import com.projectgc.calendar.repository.etl.resolveGameReleaseReferences
import com.projectgc.calendar.repository.etl.resolveGameVideoReferences
import com.projectgc.calendar.repository.etl.resolveScreenshotReferences
import com.projectgc.calendar.repository.etl.resolveWebsiteReferences
import org.springframework.stereotype.Service

@Service
class AffectedGameIdCalculator(
    private val ingestEtlReadJdbcRepository: IngestEtlReadJdbcRepository,
    private val serviceEtlJdbcRepository: ServiceEtlJdbcRepository,
) {
    companion object {
        private const val GAME_PROJECTION_DIFF_NOTE =
            "slice6 affected game_id diff calculated from service.game core and bridge projections"
        private const val GAME_RELEASE_DIFF_NOTE =
            "slice6 affected game_id diff calculated from service.game_release projection"
        private const val INVOLVED_COMPANY_DIFF_NOTE =
            "slice6 affected game_id diff calculated from service.game_company projection"
        private const val LANGUAGE_SUPPORT_DIFF_NOTE =
            "slice6 affected game_id diff calculated from service.game_language projection"
        private const val GAME_LOCALIZATION_DIFF_NOTE =
            "slice6 affected game_id diff calculated from service.game_localization projection"
        private const val COVER_DIFF_NOTE =
            "slice6 affected game_id diff calculated from service.cover projection"
        private const val ARTWORK_DIFF_NOTE =
            "slice6 affected game_id diff calculated from service.artwork projection"
        private const val SCREENSHOT_DIFF_NOTE =
            "slice6 affected game_id diff calculated from service.screenshot projection"
        private const val GAME_VIDEO_DIFF_NOTE =
            "slice6 affected game_id diff calculated from service.game_video projection"
        private const val WEBSITE_DIFF_NOTE =
            "slice6 affected game_id diff calculated from service.website projection"
        private const val ALTERNATIVE_NAME_DIFF_NOTE =
            "slice6 affected game_id diff calculated from service.alternative_name projection"
    }

    fun prepare(@Suppress("UNUSED_PARAMETER") syncStartedAt: Long): PreparedAffectedGameIdInputs {
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
            coverRows = ingestEtlReadJdbcRepository.loadAllCoverProjectionRows(),
            artworkRows = ingestEtlReadJdbcRepository.loadAllArtworkProjectionRows(),
            screenshotRows = ingestEtlReadJdbcRepository.loadAllScreenshotProjectionRows(),
            gameVideoRows = ingestEtlReadJdbcRepository.loadAllGameVideoProjectionRows(),
            websiteRows = ingestEtlReadJdbcRepository.loadAllWebsiteProjectionRows(),
            alternativeNameRows = ingestEtlReadJdbcRepository.loadAllAlternativeNameProjectionRows(),
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
            projectionDiffResult(
                tableName = "cover",
                note = COVER_DIFF_NOTE,
                affectedGameIds = findAffectedGameIdsFromCoverProjectionDiff(
                    ingestGameIds = allGameIds,
                    preparedInputs = preparedInputs,
                ),
            ),
            projectionDiffResult(
                tableName = "artwork",
                note = ARTWORK_DIFF_NOTE,
                affectedGameIds = findAffectedGameIdsFromArtworkProjectionDiff(
                    ingestGameIds = allGameIds,
                    artworkRows = preparedInputs.artworkRows,
                ),
            ),
            projectionDiffResult(
                tableName = "screenshot",
                note = SCREENSHOT_DIFF_NOTE,
                affectedGameIds = findAffectedGameIdsFromScreenshotProjectionDiff(
                    ingestGameIds = allGameIds,
                    screenshotRows = preparedInputs.screenshotRows,
                ),
            ),
            projectionDiffResult(
                tableName = "game_video",
                note = GAME_VIDEO_DIFF_NOTE,
                affectedGameIds = findAffectedGameIdsFromGameVideoProjectionDiff(
                    ingestGameIds = allGameIds,
                    gameVideoRows = preparedInputs.gameVideoRows,
                ),
            ),
            projectionDiffResult(
                tableName = "website",
                note = WEBSITE_DIFF_NOTE,
                affectedGameIds = findAffectedGameIdsFromWebsiteProjectionDiff(
                    ingestGameIds = allGameIds,
                    websiteRows = preparedInputs.websiteRows,
                ),
            ),
            projectionDiffResult(
                tableName = "alternative_name",
                note = ALTERNATIVE_NAME_DIFF_NOTE,
                affectedGameIds = findAffectedGameIdsFromAlternativeNameProjectionDiff(
                    ingestGameIds = allGameIds,
                    alternativeNameRows = preparedInputs.alternativeNameRows,
                ),
            ),
        )

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

    private fun findAffectedGameIdsFromCoverProjectionDiff(
        ingestGameIds: Set<Long>,
        preparedInputs: PreparedAffectedGameIdInputs,
    ): Set<Long> {
        val expectedLocalizationsById = resolveGameLocalizationReferences(
            rows = preparedInputs.gameLocalizationRows,
            availableGameIds = ingestGameIds,
            availableRegionIds = serviceEtlJdbcRepository.loadIds("service.region"),
        ).associate { it.id to it.gameId }
        val expectedRows = resolveCoverReferences(
            rows = preparedInputs.coverRows,
            availableGameIds = ingestGameIds,
            availableGameLocalizationsById = expectedLocalizationsById,
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentCoverProjectionRows()
        return findAffectedGameIdsByKey(
            expectedRows = expectedRows,
            actualRows = actualRows,
            keySelector = { it.id },
            gameIdSelector = { it.gameId },
            includeActualGameId = { it in ingestGameIds },
        )
    }

    private fun findAffectedGameIdsFromArtworkProjectionDiff(
        ingestGameIds: Set<Long>,
        artworkRows: List<ArtworkProjectionRow>,
    ): Set<Long> {
        val expectedRows = resolveArtworkReferences(
            rows = artworkRows,
            availableGameIds = ingestGameIds,
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentArtworkProjectionRows()
        return findAffectedGameIdsByKey(
            expectedRows = expectedRows,
            actualRows = actualRows,
            keySelector = { it.id },
            gameIdSelector = { it.gameId },
            includeActualGameId = { it in ingestGameIds },
        )
    }

    private fun findAffectedGameIdsFromScreenshotProjectionDiff(
        ingestGameIds: Set<Long>,
        screenshotRows: List<ScreenshotProjectionRow>,
    ): Set<Long> {
        val expectedRows = resolveScreenshotReferences(
            rows = screenshotRows,
            availableGameIds = ingestGameIds,
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentScreenshotProjectionRows()
        return findAffectedGameIdsByKey(
            expectedRows = expectedRows,
            actualRows = actualRows,
            keySelector = { it.id },
            gameIdSelector = { it.gameId },
            includeActualGameId = { it in ingestGameIds },
        )
    }

    private fun findAffectedGameIdsFromGameVideoProjectionDiff(
        ingestGameIds: Set<Long>,
        gameVideoRows: List<GameVideoProjectionRow>,
    ): Set<Long> {
        val expectedRows = resolveGameVideoReferences(
            rows = gameVideoRows,
            availableGameIds = ingestGameIds,
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentGameVideoProjectionRows()
        return findAffectedGameIdsByKey(
            expectedRows = expectedRows,
            actualRows = actualRows,
            keySelector = { it.id },
            gameIdSelector = { it.gameId },
            includeActualGameId = { it in ingestGameIds },
        )
    }

    private fun findAffectedGameIdsFromWebsiteProjectionDiff(
        ingestGameIds: Set<Long>,
        websiteRows: List<WebsiteProjectionRow>,
    ): Set<Long> {
        val expectedRows = resolveWebsiteReferences(
            rows = websiteRows,
            availableGameIds = ingestGameIds,
            availableTypeIds = serviceEtlJdbcRepository.loadIds("service.website_type"),
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentWebsiteProjectionRows()
        return findAffectedGameIdsByKey(
            expectedRows = expectedRows,
            actualRows = actualRows,
            keySelector = { it.id },
            gameIdSelector = { it.gameId },
            includeActualGameId = { it in ingestGameIds },
        )
    }

    private fun findAffectedGameIdsFromAlternativeNameProjectionDiff(
        ingestGameIds: Set<Long>,
        alternativeNameRows: List<AlternativeNameProjectionRow>,
    ): Set<Long> {
        val expectedRows = resolveAlternativeNameReferences(
            rows = alternativeNameRows,
            availableGameIds = ingestGameIds,
        )
        val actualRows = serviceEtlJdbcRepository.loadCurrentAlternativeNameProjectionRows()
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
    val coverRows: List<CoverProjectionRow>,
    val artworkRows: List<ArtworkProjectionRow>,
    val screenshotRows: List<ScreenshotProjectionRow>,
    val gameVideoRows: List<GameVideoProjectionRow>,
    val websiteRows: List<WebsiteProjectionRow>,
    val alternativeNameRows: List<AlternativeNameProjectionRow>,
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
