package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.AlternativeNameProjectionRow
import com.projectgc.calendar.repository.etl.ArtworkProjectionRow
import com.projectgc.calendar.repository.etl.CompanySyncRow
import com.projectgc.calendar.repository.etl.CoverProjectionRow
import com.projectgc.calendar.repository.etl.GameCompanyProjectionRow
import com.projectgc.calendar.repository.etl.GameDimensionProjectionRow
import com.projectgc.calendar.repository.etl.GameLanguageProjectionRow
import com.projectgc.calendar.repository.etl.GameLocalizationProjectionRow
import com.projectgc.calendar.repository.etl.GameProjectionRow
import com.projectgc.calendar.repository.etl.GameRelationProjectionRow
import com.projectgc.calendar.repository.etl.GameReleaseProjectionRow
import com.projectgc.calendar.repository.etl.GameVideoProjectionRow
import com.projectgc.calendar.repository.etl.IngestEtlReadJdbcRepository
import com.projectgc.calendar.repository.etl.LanguageRow
import com.projectgc.calendar.repository.etl.NamedDimensionRow
import com.projectgc.calendar.repository.etl.PlatformLogoRow
import com.projectgc.calendar.repository.etl.PlatformSyncRow
import com.projectgc.calendar.repository.etl.RegionRow
import com.projectgc.calendar.repository.etl.ReleaseStatusRow
import com.projectgc.calendar.repository.etl.ScreenshotProjectionRow
import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import com.projectgc.calendar.repository.etl.ServiceEtlMismatchLogEntry
import com.projectgc.calendar.repository.etl.ServiceEtlSourceLogEntry
import com.projectgc.calendar.repository.etl.ServiceEtlTableSyncResult
import com.projectgc.calendar.repository.etl.WebsiteProjectionRow
import com.projectgc.calendar.repository.etl.resolveAlternativeNameReferences
import com.projectgc.calendar.repository.etl.resolveArtworkReferences
import com.projectgc.calendar.repository.etl.resolveCompanyReferences
import com.projectgc.calendar.repository.etl.resolveCoverReferences
import com.projectgc.calendar.repository.etl.resolveGameCompanyReferences
import com.projectgc.calendar.repository.etl.resolveGameDimensionReferences
import com.projectgc.calendar.repository.etl.resolveGameLanguageReferences
import com.projectgc.calendar.repository.etl.resolveGameLocalizationReferences
import com.projectgc.calendar.repository.etl.resolveGameReferences
import com.projectgc.calendar.repository.etl.resolveGameRelationReferences
import com.projectgc.calendar.repository.etl.resolveGameReleaseReferences
import com.projectgc.calendar.repository.etl.resolveGameVideoReferences
import com.projectgc.calendar.repository.etl.resolvePlatformReferences
import com.projectgc.calendar.repository.etl.resolveScreenshotReferences
import com.projectgc.calendar.repository.etl.resolveWebsiteReferences
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID

private const val VALIDATION_SAMPLE_LIMIT = 100

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
        private const val MAX_ATTEMPTS = 2
        private const val SLICE7_GAME_PROJECTION_NOTE =
            "slice7 projections rebuilt and validated: root deletions, dimension pruning, affected-scope diff validation, mismatch rollback, single retry"
    }

    override fun run(runId: UUID, trigger: ServiceEtlTrigger) {
        val startedAt = Instant.now()
        log.info("service ETL 시작 (runId=$runId, trigger=${trigger.type}, ingestSyncId=${trigger.ingestSyncId})")
        serviceEtlJdbcRepository.insertRunLog(runId, trigger, startedAt)

        try {
            val preparedSlice2Inputs = prepareSlice2Inputs()
            val preparedAffectedGameInputs = affectedGameIdCalculator.prepare(startedAt.epochSecond)
            val affectedGameCount = executeWithRetry(
                runId = runId,
                preparedSlice2Inputs = preparedSlice2Inputs,
                preparedAffectedGameInputs = preparedAffectedGameInputs,
            )

            serviceEtlJdbcRepository.finishRunLog(
                runId = runId,
                finishedAt = Instant.now(),
                status = COMPLETED,
            )
            log.info("service ETL 완료 (runId=$runId, affectedGames=$affectedGameCount)")
        } catch (ex: Exception) {
            persistValidationMismatchesIfNeeded(runId, ex)
            runCatching {
                val validationException = ex as? ServiceEtlValidationException
                serviceEtlJdbcRepository.finishRunLog(
                    runId = runId,
                    finishedAt = Instant.now(),
                    status = FAILED,
                    mismatchCount = validationException?.mismatchCount ?: 0,
                    errorMessage = ex.message?.take(1000),
                )
            }.onFailure { finishError ->
                log.error("service ETL 실패 로그 저장 실패 (runId=$runId): ${finishError.message}", finishError)
            }
            log.error("service ETL 실패 (runId=$runId): ${ex.message}", ex)
            throw ex
        }
    }

    private fun prepareSlice2Inputs() = PreparedSlice2Inputs(
        gameStatuses = ingestEtlReadJdbcRepository.loadGameStatuses(),
        gameTypes = ingestEtlReadJdbcRepository.loadGameTypes(),
        languages = ingestEtlReadJdbcRepository.loadLanguages(),
        regions = ingestEtlReadJdbcRepository.loadRegions(),
        releaseRegions = ingestEtlReadJdbcRepository.loadReleaseRegions(),
        releaseStatuses = ingestEtlReadJdbcRepository.loadReleaseStatuses(),
        genres = ingestEtlReadJdbcRepository.loadGenres(),
        themes = ingestEtlReadJdbcRepository.loadThemes(),
        playerPerspectives = ingestEtlReadJdbcRepository.loadPlayerPerspectives(),
        gameModes = ingestEtlReadJdbcRepository.loadGameModes(),
        keywords = ingestEtlReadJdbcRepository.loadKeywords(),
        languageSupportTypes = ingestEtlReadJdbcRepository.loadLanguageSupportTypes(),
        websiteTypes = ingestEtlReadJdbcRepository.loadWebsiteTypes(),
        platformLogos = ingestEtlReadJdbcRepository.loadPlatformLogos(),
        platformTypes = ingestEtlReadJdbcRepository.loadPlatformTypes(),
        platforms = ingestEtlReadJdbcRepository.loadPlatforms(),
        companies = ingestEtlReadJdbcRepository.loadCompanies(),
    )

    private fun executeWithRetry(
        runId: UUID,
        preparedSlice2Inputs: PreparedSlice2Inputs,
        preparedAffectedGameInputs: PreparedAffectedGameIdInputs,
    ): Int {
        var attempt = 1
        while (true) {
            try {
                var affectedGameCount = 0
                transactionTemplate.executeWithoutResult {
                    syncSlice2Sources(runId, preparedSlice2Inputs)
                    val calculationResult = affectedGameIdCalculator.calculate(preparedAffectedGameInputs)
                    affectedGameCount = rebuildAndValidate(
                        runId = runId,
                        preparedSlice2Inputs = preparedSlice2Inputs,
                        preparedAffectedGameInputs = preparedAffectedGameInputs,
                        calculationResult = calculationResult,
                    )
                }
                return affectedGameCount
            } catch (ex: Exception) {
                if (attempt >= MAX_ATTEMPTS) {
                    throw ex
                }
                log.warn(
                    "service ETL attempt 실패, 1회 재시도 (runId=$runId, attempt=$attempt/$MAX_ATTEMPTS, error=${ex.message})"
                )
                attempt += 1
            }
        }
    }

    private fun syncSlice2Sources(runId: UUID, inputs: PreparedSlice2Inputs) {
        val idSets = inputs.idSets()
        syncSourceTable(runId, "game_status", inputs.gameStatuses, serviceEtlJdbcRepository::syncGameStatuses)
        syncSourceTable(runId, "game_type", inputs.gameTypes, serviceEtlJdbcRepository::syncGameTypes)
        syncSourceTable(runId, "language", inputs.languages, serviceEtlJdbcRepository::syncLanguages)
        syncSourceTable(runId, "region", inputs.regions, serviceEtlJdbcRepository::syncRegions)
        syncSourceTable(runId, "release_date_region", inputs.releaseRegions, serviceEtlJdbcRepository::syncReleaseRegions)
        syncSourceTable(runId, "release_date_status", inputs.releaseStatuses, serviceEtlJdbcRepository::syncReleaseStatuses)
        syncSourceTable(runId, "genre", inputs.genres, serviceEtlJdbcRepository::syncGenres)
        syncSourceTable(runId, "theme", inputs.themes, serviceEtlJdbcRepository::syncThemes)
        syncSourceTable(
            runId,
            "player_perspective",
            inputs.playerPerspectives,
            serviceEtlJdbcRepository::syncPlayerPerspectives,
        )
        syncSourceTable(runId, "game_mode", inputs.gameModes, serviceEtlJdbcRepository::syncGameModes)
        syncSourceTable(runId, "keyword", inputs.keywords, serviceEtlJdbcRepository::syncKeywords)
        syncSourceTable(
            runId,
            "language_support_type",
            inputs.languageSupportTypes,
            serviceEtlJdbcRepository::syncLanguageSupportTypes,
        )
        syncSourceTable(runId, "website_type", inputs.websiteTypes, serviceEtlJdbcRepository::syncWebsiteTypes)
        syncSourceTable(runId, "platform_logo", inputs.platformLogos, serviceEtlJdbcRepository::syncPlatformLogos)
        syncSourceTable(runId, "platform_type", inputs.platformTypes, serviceEtlJdbcRepository::syncPlatformTypes)
        syncSourceTable(runId, "platform", inputs.platforms) { sourceRows ->
            serviceEtlJdbcRepository.syncPlatforms(
                sourceRows = sourceRows,
                availableLogoIds = idSets.platformLogoIds,
                availableTypeIds = idSets.platformTypeIds,
            )
        }
        syncSourceTable(runId, "company", inputs.companies, serviceEtlJdbcRepository::syncCompanies)
    }

    private fun rebuildAndValidate(
        runId: UUID,
        preparedSlice2Inputs: PreparedSlice2Inputs,
        preparedAffectedGameInputs: PreparedAffectedGameIdInputs,
        calculationResult: AffectedGameIdCalculationResult,
    ): Int {
        val slice2IdSets = preparedSlice2Inputs.idSets()
        val dimensionDeletionAffectedGameIds = findSharedDimensionDeletionAffectedGameIds(
            preparedSlice2Inputs = preparedSlice2Inputs,
            sourceGameIds = preparedAffectedGameInputs.allGameIds,
        )
        val affectedGameCount = rebuildAffectedGameProjections(
            runId = runId,
            preparedInputs = preparedAffectedGameInputs,
            calculationResult = calculationResult,
            dimensionDeletionAffectedGameIds = dimensionDeletionAffectedGameIds,
            slice2IdSets = slice2IdSets,
        )
        val deletedGameIds = deleteMissingRootGames(preparedAffectedGameInputs.allGameIds)
        pruneMissingSharedDimensions(preparedSlice2Inputs)

        val validationGameIds = linkedSetOf<Long>().apply {
            addAll(calculationResult.affectedGameIds)
            addAll(dimensionDeletionAffectedGameIds)
            addAll(deletedGameIds)
        }
        val validationResult = validateFinalState(
            preparedSlice2Inputs = preparedSlice2Inputs,
            preparedAffectedGameInputs = preparedAffectedGameInputs,
            validationGameIds = validationGameIds,
        )
        if (validationResult.mismatchCount > 0) {
            throw ServiceEtlValidationException(
                mismatchCount = validationResult.mismatchCount,
                mismatchSamples = validationResult.sampleMismatches,
            )
        }

        return affectedGameCount
    }

    private fun deleteMissingRootGames(expectedGameIds: Set<Long>): Set<Long> {
        val currentGameIds = serviceEtlJdbcRepository.loadIds("service.game")
        val deletedGameIds = currentGameIds - expectedGameIds
        serviceEtlJdbcRepository.deleteIds("service.game", deletedGameIds)
        return deletedGameIds
    }

    private fun pruneMissingSharedDimensions(inputs: PreparedSlice2Inputs) {
        pruneTableByIds("service.game_status", idsOf(inputs.gameStatuses) { it.id })
        pruneTableByIds("service.game_type", idsOf(inputs.gameTypes) { it.id })
        pruneTableByIds("service.language", idsOf(inputs.languages) { it.id })
        pruneTableByIds("service.region", idsOf(inputs.regions) { it.id })
        pruneTableByIds("service.release_region", idsOf(inputs.releaseRegions) { it.id })
        pruneTableByIds("service.release_status", idsOf(inputs.releaseStatuses) { it.id })
        pruneTableByIds("service.genre", idsOf(inputs.genres) { it.id })
        pruneTableByIds("service.theme", idsOf(inputs.themes) { it.id })
        pruneTableByIds("service.player_perspective", idsOf(inputs.playerPerspectives) { it.id })
        pruneTableByIds("service.game_mode", idsOf(inputs.gameModes) { it.id })
        pruneTableByIds("service.keyword", idsOf(inputs.keywords) { it.id })
        pruneTableByIds("service.language_support_type", idsOf(inputs.languageSupportTypes) { it.id })
        pruneTableByIds("service.website_type", idsOf(inputs.websiteTypes) { it.id })
        pruneTableByIds("service.platform", idsOf(inputs.platforms) { it.id })
        pruneTableByIds("service.platform_logo", idsOf(inputs.platformLogos) { it.id })
        pruneTableByIds("service.platform_type", idsOf(inputs.platformTypes) { it.id })
        pruneTableByIds("service.company", idsOf(inputs.companies) { it.id })
    }

    private fun pruneTableByIds(tableName: String, expectedIds: Set<Long>) {
        val actualIds = serviceEtlJdbcRepository.loadIds(tableName)
        val idsToDelete = actualIds - expectedIds
        serviceEtlJdbcRepository.deleteIds(tableName, idsToDelete)
    }

    private fun findSharedDimensionDeletionAffectedGameIds(
        preparedSlice2Inputs: PreparedSlice2Inputs,
        sourceGameIds: Set<Long>,
    ): Set<Long> {
        val idSets = preparedSlice2Inputs.idSets()
        val affectedGameIds = linkedSetOf<Long>()

        serviceEtlJdbcRepository.loadCurrentGameProjectionRows().forEach { row ->
            if (
                (row.statusId != null && row.statusId !in idSets.gameStatusIds) ||
                (row.typeId != null && row.typeId !in idSets.gameTypeIds)
            ) {
                affectedGameIds += row.id
            }
        }
        serviceEtlJdbcRepository.loadCurrentGameLocalizationProjectionRows().forEach { row ->
            if (row.regionId != null && row.regionId !in idSets.regionIds) {
                affectedGameIds += row.gameId
            }
        }
        serviceEtlJdbcRepository.loadCurrentGameReleaseProjectionRows().forEach { row ->
            if (
                (row.platformId != null && row.platformId !in idSets.platformIds) ||
                (row.regionId != null && row.regionId !in idSets.releaseRegionIds) ||
                (row.statusId != null && row.statusId !in idSets.releaseStatusIds)
            ) {
                affectedGameIds += row.gameId
            }
        }
        serviceEtlJdbcRepository.loadCurrentGameLanguageProjectionRows().forEach { row ->
            if (row.languageId !in idSets.languageIds) {
                affectedGameIds += row.gameId
            }
        }
        collectDimensionDeletionAffectedGameIds(
            rows = serviceEtlJdbcRepository.loadCurrentGameDimensionProjectionRows("game_genre", "genre_id"),
            availableDimensionIds = idSets.genreIds,
            affectedGameIds = affectedGameIds,
        )
        collectDimensionDeletionAffectedGameIds(
            rows = serviceEtlJdbcRepository.loadCurrentGameDimensionProjectionRows("game_theme", "theme_id"),
            availableDimensionIds = idSets.themeIds,
            affectedGameIds = affectedGameIds,
        )
        collectDimensionDeletionAffectedGameIds(
            rows = serviceEtlJdbcRepository.loadCurrentGameDimensionProjectionRows(
                "game_player_perspective",
                "player_perspective_id",
            ),
            availableDimensionIds = idSets.playerPerspectiveIds,
            affectedGameIds = affectedGameIds,
        )
        collectDimensionDeletionAffectedGameIds(
            rows = serviceEtlJdbcRepository.loadCurrentGameDimensionProjectionRows("game_game_mode", "game_mode_id"),
            availableDimensionIds = idSets.gameModeIds,
            affectedGameIds = affectedGameIds,
        )
        collectDimensionDeletionAffectedGameIds(
            rows = serviceEtlJdbcRepository.loadCurrentGameDimensionProjectionRows("game_keyword", "keyword_id"),
            availableDimensionIds = idSets.keywordIds,
            affectedGameIds = affectedGameIds,
        )
        serviceEtlJdbcRepository.loadCurrentGameCompanyProjectionRows().forEach { row ->
            if (row.companyId !in idSets.companyIds) {
                affectedGameIds += row.gameId
            }
        }
        serviceEtlJdbcRepository.loadCurrentWebsiteProjectionRows().forEach { row ->
            if (row.typeId != null && row.typeId !in idSets.websiteTypeIds) {
                affectedGameIds += row.gameId
            }
        }

        return affectedGameIds.filterTo(linkedSetOf()) { it in sourceGameIds }
    }

    private fun collectDimensionDeletionAffectedGameIds(
        rows: List<GameDimensionProjectionRow>,
        availableDimensionIds: Set<Long>,
        affectedGameIds: MutableSet<Long>,
    ) {
        rows.forEach { row ->
            if (row.dimensionId !in availableDimensionIds) {
                affectedGameIds += row.gameId
            }
        }
    }

    private fun validateFinalState(
        preparedSlice2Inputs: PreparedSlice2Inputs,
        preparedAffectedGameInputs: PreparedAffectedGameIdInputs,
        validationGameIds: Set<Long>,
    ): ServiceEtlValidationResult {
        val accumulator = ValidationAccumulator()

        validateSlice2Dimensions(preparedSlice2Inputs, accumulator)
        validateGameProjections(
            preparedSlice2Inputs = preparedSlice2Inputs,
            preparedAffectedGameInputs = preparedAffectedGameInputs,
            validationGameIds = validationGameIds,
            accumulator = accumulator,
        )

        return accumulator.toResult()
    }

    private fun validateSlice2Dimensions(
        inputs: PreparedSlice2Inputs,
        accumulator: ValidationAccumulator,
    ) {
        accumulator.collect(
            tableName = "service.game_status",
            expectedRows = sortRowsById(inputs.gameStatuses) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentNamedDimensionRows("game_status", "status"),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.game_type",
            expectedRows = sortRowsById(inputs.gameTypes) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentNamedDimensionRows("game_type", "type"),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.language",
            expectedRows = sortRowsById(inputs.languages) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentLanguages(),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.region",
            expectedRows = sortRowsById(inputs.regions) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentRegions(),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.release_region",
            expectedRows = sortRowsById(inputs.releaseRegions) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentNamedDimensionRows("release_region", "name"),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.release_status",
            expectedRows = sortRowsById(inputs.releaseStatuses) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentReleaseStatuses(),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.genre",
            expectedRows = sortRowsById(inputs.genres) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentNamedDimensionRows("genre", "name"),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.theme",
            expectedRows = sortRowsById(inputs.themes) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentNamedDimensionRows("theme", "name"),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.player_perspective",
            expectedRows = sortRowsById(inputs.playerPerspectives) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentNamedDimensionRows("player_perspective", "name"),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.game_mode",
            expectedRows = sortRowsById(inputs.gameModes) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentNamedDimensionRows("game_mode", "name"),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.keyword",
            expectedRows = sortRowsById(inputs.keywords) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentNamedDimensionRows("keyword", "name"),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.language_support_type",
            expectedRows = sortRowsById(inputs.languageSupportTypes) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentNamedDimensionRows("language_support_type", "name"),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.website_type",
            expectedRows = sortRowsById(inputs.websiteTypes) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentNamedDimensionRows("website_type", "type"),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.platform_logo",
            expectedRows = sortRowsById(inputs.platformLogos) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentPlatformLogos(),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.platform_type",
            expectedRows = sortRowsById(inputs.platformTypes) { it.id },
            actualRows = serviceEtlJdbcRepository.loadCurrentNamedDimensionRows("platform_type", "name"),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.platform",
            expectedRows = resolvePlatformReferences(
                rows = inputs.platforms,
                availableLogoIds = idsOf(inputs.platformLogos) { it.id },
                availableTypeIds = idsOf(inputs.platformTypes) { it.id },
            ).let { sortRowsById(it) { row -> row.id } },
            actualRows = serviceEtlJdbcRepository.loadCurrentPlatforms(),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.company",
            expectedRows = resolveCompanyReferences(inputs.companies).let { sortRowsById(it) { row -> row.id } },
            actualRows = serviceEtlJdbcRepository.loadCurrentCompanies(),
            keySelector = { it.id },
        )
    }

    private fun validateGameProjections(
        preparedSlice2Inputs: PreparedSlice2Inputs,
        preparedAffectedGameInputs: PreparedAffectedGameIdInputs,
        validationGameIds: Set<Long>,
        accumulator: ValidationAccumulator,
    ) {
        val allGameIds = preparedAffectedGameInputs.allGameIds
        val idSets = preparedSlice2Inputs.idSets()

        val expectedGameRows = resolveGameReferences(
            rows = preparedAffectedGameInputs.gameRows,
            availableStatusIds = idSets.gameStatusIds,
            availableTypeIds = idSets.gameTypeIds,
        ).filterGameRowsByGameIds(validationGameIds)
        accumulator.collect(
            tableName = "service.game",
            expectedRows = expectedGameRows,
            actualRows = serviceEtlJdbcRepository.loadCurrentGameProjectionRows().filterGameRowsByGameIds(validationGameIds),
            keySelector = { it.id },
        )

        val resolvedGameLocalizationRows = resolveGameLocalizationReferences(
            rows = preparedAffectedGameInputs.gameLocalizationRows,
            availableGameIds = allGameIds,
            availableRegionIds = idSets.regionIds,
        )
        accumulator.collect(
            tableName = "service.game_localization",
            expectedRows = resolvedGameLocalizationRows.filterLocalizationRowsByGameIds(validationGameIds),
            actualRows = serviceEtlJdbcRepository.loadCurrentGameLocalizationProjectionRows()
                .filterLocalizationRowsByGameIds(validationGameIds),
            keySelector = { it.id },
        )

        accumulator.collect(
            tableName = "service.game_release",
            expectedRows = resolveGameReleaseReferences(
                rows = preparedAffectedGameInputs.gameReleaseRows,
                availableGameIds = allGameIds,
                availablePlatformIds = idSets.platformIds,
                availableRegionIds = idSets.releaseRegionIds,
                availableStatusIds = idSets.releaseStatusIds,
            ).filterReleaseRowsByGameIds(validationGameIds),
            actualRows = serviceEtlJdbcRepository.loadCurrentGameReleaseProjectionRows()
                .filterReleaseRowsByGameIds(validationGameIds),
            keySelector = { it.id },
        )

        accumulator.collect(
            tableName = "service.game_language",
            expectedRows = resolveGameLanguageReferences(
                rows = preparedAffectedGameInputs.gameLanguageRows,
                availableGameIds = allGameIds,
                availableLanguageIds = idSets.languageIds,
            ).filterLanguageRowsByGameIds(validationGameIds),
            actualRows = serviceEtlJdbcRepository.loadCurrentGameLanguageProjectionRows()
                .filterLanguageRowsByGameIds(validationGameIds),
            keySelector = { it.gameId to it.languageId },
        )

        validateGameDimensionProjection(
            tableName = "service.game_genre",
            expectedRows = preparedAffectedGameInputs.gameGenreRows,
            actualRows = serviceEtlJdbcRepository.loadCurrentGameDimensionProjectionRows("game_genre", "genre_id"),
            availableGameIds = allGameIds,
            availableDimensionIds = idSets.genreIds,
            validationGameIds = validationGameIds,
            accumulator = accumulator,
        )
        validateGameDimensionProjection(
            tableName = "service.game_theme",
            expectedRows = preparedAffectedGameInputs.gameThemeRows,
            actualRows = serviceEtlJdbcRepository.loadCurrentGameDimensionProjectionRows("game_theme", "theme_id"),
            availableGameIds = allGameIds,
            availableDimensionIds = idSets.themeIds,
            validationGameIds = validationGameIds,
            accumulator = accumulator,
        )
        validateGameDimensionProjection(
            tableName = "service.game_player_perspective",
            expectedRows = preparedAffectedGameInputs.gamePlayerPerspectiveRows,
            actualRows = serviceEtlJdbcRepository.loadCurrentGameDimensionProjectionRows(
                "game_player_perspective",
                "player_perspective_id",
            ),
            availableGameIds = allGameIds,
            availableDimensionIds = idSets.playerPerspectiveIds,
            validationGameIds = validationGameIds,
            accumulator = accumulator,
        )
        validateGameDimensionProjection(
            tableName = "service.game_game_mode",
            expectedRows = preparedAffectedGameInputs.gameModeRows,
            actualRows = serviceEtlJdbcRepository.loadCurrentGameDimensionProjectionRows("game_game_mode", "game_mode_id"),
            availableGameIds = allGameIds,
            availableDimensionIds = idSets.gameModeIds,
            validationGameIds = validationGameIds,
            accumulator = accumulator,
        )
        validateGameDimensionProjection(
            tableName = "service.game_keyword",
            expectedRows = preparedAffectedGameInputs.gameKeywordRows,
            actualRows = serviceEtlJdbcRepository.loadCurrentGameDimensionProjectionRows("game_keyword", "keyword_id"),
            availableGameIds = allGameIds,
            availableDimensionIds = idSets.keywordIds,
            validationGameIds = validationGameIds,
            accumulator = accumulator,
        )

        accumulator.collect(
            tableName = "service.game_company",
            expectedRows = resolveGameCompanyReferences(
                rows = preparedAffectedGameInputs.gameCompanyRows,
                availableGameIds = allGameIds,
                availableCompanyIds = idSets.companyIds,
            ).filterCompanyRowsByGameIds(validationGameIds),
            actualRows = serviceEtlJdbcRepository.loadCurrentGameCompanyProjectionRows()
                .filterCompanyRowsByGameIds(validationGameIds),
            keySelector = { it.gameId to it.companyId },
        )

        accumulator.collect(
            tableName = "service.game_relation",
            expectedRows = resolveGameRelationReferences(
                rows = preparedAffectedGameInputs.gameRelationRows,
                availableGameIds = allGameIds,
            ).filterRelationRowsByGameIds(validationGameIds),
            actualRows = serviceEtlJdbcRepository.loadCurrentGameRelationProjectionRows()
                .filterRelationRowsByGameIds(validationGameIds),
            keySelector = { Triple(it.gameId, it.relatedGameId, it.relationType) },
        )

        val expectedGameLocalizationById = resolvedGameLocalizationRows.associate { it.id to it.gameId }
        accumulator.collect(
            tableName = "service.cover",
            expectedRows = resolveCoverReferences(
                rows = preparedAffectedGameInputs.coverRows,
                availableGameIds = allGameIds,
                availableGameLocalizationsById = expectedGameLocalizationById,
            ).filterCoverRowsByGameIds(validationGameIds),
            actualRows = serviceEtlJdbcRepository.loadCurrentCoverProjectionRows()
                .filterCoverRowsByGameIds(validationGameIds),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.artwork",
            expectedRows = resolveArtworkReferences(
                rows = preparedAffectedGameInputs.artworkRows,
                availableGameIds = allGameIds,
            ).filterArtworkRowsByGameIds(validationGameIds),
            actualRows = serviceEtlJdbcRepository.loadCurrentArtworkProjectionRows()
                .filterArtworkRowsByGameIds(validationGameIds),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.screenshot",
            expectedRows = resolveScreenshotReferences(
                rows = preparedAffectedGameInputs.screenshotRows,
                availableGameIds = allGameIds,
            ).filterScreenshotRowsByGameIds(validationGameIds),
            actualRows = serviceEtlJdbcRepository.loadCurrentScreenshotProjectionRows()
                .filterScreenshotRowsByGameIds(validationGameIds),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.game_video",
            expectedRows = resolveGameVideoReferences(
                rows = preparedAffectedGameInputs.gameVideoRows,
                availableGameIds = allGameIds,
            ).filterGameVideoRowsByGameIds(validationGameIds),
            actualRows = serviceEtlJdbcRepository.loadCurrentGameVideoProjectionRows()
                .filterGameVideoRowsByGameIds(validationGameIds),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.website",
            expectedRows = resolveWebsiteReferences(
                rows = preparedAffectedGameInputs.websiteRows,
                availableGameIds = allGameIds,
                availableTypeIds = idSets.websiteTypeIds,
            ).filterWebsiteRowsByGameIds(validationGameIds),
            actualRows = serviceEtlJdbcRepository.loadCurrentWebsiteProjectionRows()
                .filterWebsiteRowsByGameIds(validationGameIds),
            keySelector = { it.id },
        )
        accumulator.collect(
            tableName = "service.alternative_name",
            expectedRows = resolveAlternativeNameReferences(
                rows = preparedAffectedGameInputs.alternativeNameRows,
                availableGameIds = allGameIds,
            ).filterAlternativeNameRowsByGameIds(validationGameIds),
            actualRows = serviceEtlJdbcRepository.loadCurrentAlternativeNameProjectionRows()
                .filterAlternativeNameRowsByGameIds(validationGameIds),
            keySelector = { it.id },
        )
    }

    private fun validateGameDimensionProjection(
        tableName: String,
        expectedRows: List<GameDimensionProjectionRow>,
        actualRows: List<GameDimensionProjectionRow>,
        availableGameIds: Set<Long>,
        availableDimensionIds: Set<Long>,
        validationGameIds: Set<Long>,
        accumulator: ValidationAccumulator,
    ) {
        accumulator.collect(
            tableName = tableName,
            expectedRows = resolveGameDimensionReferences(
                rows = expectedRows,
                availableGameIds = availableGameIds,
                availableDimensionIds = availableDimensionIds,
            ).filterDimensionRowsByGameIds(validationGameIds),
            actualRows = actualRows.filterDimensionRowsByGameIds(validationGameIds),
            keySelector = { it.gameId to it.dimensionId },
        )
    }

    private fun rebuildAffectedGameProjections(
        runId: UUID,
        preparedInputs: PreparedAffectedGameIdInputs,
        calculationResult: AffectedGameIdCalculationResult,
        dimensionDeletionAffectedGameIds: Set<Long>,
        slice2IdSets: PreparedSlice2IdSets,
    ): Int {
        val requestedGameIds = linkedSetOf<Long>().apply {
            addAll(calculationResult.affectedGameIds)
            addAll(dimensionDeletionAffectedGameIds)
        }
        val sourceGameRows = preparedInputs.gameRows.filterGameRowsByGameIds(requestedGameIds)
        val materializedGameIds = sourceGameRows.mapTo(linkedSetOf()) { it.id }

        serviceEtlJdbcRepository.rebuildCoreGameProjections(
            gameRows = sourceGameRows,
            gameLocalizationRows = preparedInputs.gameLocalizationRows.filterLocalizationRowsByGameIds(materializedGameIds),
            gameReleaseRows = preparedInputs.gameReleaseRows.filterReleaseRowsByGameIds(materializedGameIds),
            availableStatusIds = slice2IdSets.gameStatusIds,
            availableTypeIds = slice2IdSets.gameTypeIds,
            availableRegionIds = slice2IdSets.regionIds,
            availablePlatformIds = slice2IdSets.platformIds,
            availableReleaseRegionIds = slice2IdSets.releaseRegionIds,
            availableReleaseStatusIds = slice2IdSets.releaseStatusIds,
        )
        val availableServiceGameIds = serviceEtlJdbcRepository.loadIds("service.game")
            .intersect(preparedInputs.allGameIds)
        val resolvedGameLocalizations = resolveGameLocalizationReferences(
            rows = preparedInputs.gameLocalizationRows.filterLocalizationRowsByGameIds(materializedGameIds),
            availableGameIds = availableServiceGameIds,
            availableRegionIds = slice2IdSets.regionIds,
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
            availableGameIds = availableServiceGameIds,
            availableLanguageIds = slice2IdSets.languageIds,
            availableGenreIds = slice2IdSets.genreIds,
            availableThemeIds = slice2IdSets.themeIds,
            availablePlayerPerspectiveIds = slice2IdSets.playerPerspectiveIds,
            availableGameModeIds = slice2IdSets.gameModeIds,
            availableKeywordIds = slice2IdSets.keywordIds,
            availableCompanyIds = slice2IdSets.companyIds,
        )
        serviceEtlJdbcRepository.rebuildGameMediaProjections(
            materializedGameIds = materializedGameIds,
            coverRows = preparedInputs.coverRows.filterCoverRowsByGameIds(materializedGameIds),
            artworkRows = preparedInputs.artworkRows.filterArtworkRowsByGameIds(materializedGameIds),
            screenshotRows = preparedInputs.screenshotRows.filterScreenshotRowsByGameIds(materializedGameIds),
            gameVideoRows = preparedInputs.gameVideoRows.filterGameVideoRowsByGameIds(materializedGameIds),
            websiteRows = preparedInputs.websiteRows.filterWebsiteRowsByGameIds(materializedGameIds),
            alternativeNameRows = preparedInputs.alternativeNameRows.filterAlternativeNameRowsByGameIds(materializedGameIds),
            availableGameIds = availableServiceGameIds,
            availableGameLocalizationsById = resolvedGameLocalizations.associate { it.id to it.gameId },
            availableWebsiteTypeIds = slice2IdSets.websiteTypeIds,
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
            serviceEtlJdbcRepository.insertSourceLog(
                ServiceEtlSourceLogEntry(
                    runId = runId,
                    tableName = sourceResult.tableName,
                    status = COMPLETED,
                    processedRows = sourceResult.affectedGameIds.size,
                    cursorFrom = sourceResult.cursorFrom,
                    cursorTo = sourceResult.cursorTo,
                    note = "${sourceResult.note}; $SLICE7_GAME_PROJECTION_NOTE",
                    startedAt = loggedAt,
                    finishedAt = loggedAt,
                )
            )
        }
        return materializedGameIds.size
    }

    private fun persistValidationMismatchesIfNeeded(runId: UUID, ex: Exception) {
        val validationException = ex as? ServiceEtlValidationException ?: return
        runCatching {
            serviceEtlJdbcRepository.insertMismatchLogs(
                validationException.mismatchSamples.map { sample ->
                    ServiceEtlMismatchLogEntry(
                        runId = runId,
                        tableName = sample.tableName,
                        mismatchType = sample.mismatchType,
                        expectedValue = sample.expectedValue,
                        actualValue = sample.actualValue,
                        detailsJson = sample.detailsJson,
                        recordedAt = Instant.now(),
                    )
                }
            )
        }.onFailure { mismatchError ->
            log.error("service ETL mismatch 로그 저장 실패 (runId=$runId): ${mismatchError.message}", mismatchError)
        }
    }

    private fun <T> syncSourceTable(
        runId: UUID,
        tableName: String,
        sourceRows: List<T>,
        syncer: (List<T>) -> ServiceEtlTableSyncResult,
    ) {
        val tableStartedAt = Instant.now()
        val result = syncer(sourceRows)
        val tableFinishedAt = Instant.now()

        serviceEtlJdbcRepository.insertSourceLog(
            ServiceEtlSourceLogEntry(
                runId = runId,
                tableName = tableName,
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
}

private data class PreparedSlice2Inputs(
    val gameStatuses: List<NamedDimensionRow>,
    val gameTypes: List<NamedDimensionRow>,
    val languages: List<LanguageRow>,
    val regions: List<RegionRow>,
    val releaseRegions: List<NamedDimensionRow>,
    val releaseStatuses: List<ReleaseStatusRow>,
    val genres: List<NamedDimensionRow>,
    val themes: List<NamedDimensionRow>,
    val playerPerspectives: List<NamedDimensionRow>,
    val gameModes: List<NamedDimensionRow>,
    val keywords: List<NamedDimensionRow>,
    val languageSupportTypes: List<NamedDimensionRow>,
    val websiteTypes: List<NamedDimensionRow>,
    val platformLogos: List<PlatformLogoRow>,
    val platformTypes: List<NamedDimensionRow>,
    val platforms: List<PlatformSyncRow>,
    val companies: List<CompanySyncRow>,
)

private data class PreparedSlice2IdSets(
    val gameStatusIds: Set<Long>,
    val gameTypeIds: Set<Long>,
    val languageIds: Set<Long>,
    val regionIds: Set<Long>,
    val releaseRegionIds: Set<Long>,
    val releaseStatusIds: Set<Long>,
    val genreIds: Set<Long>,
    val themeIds: Set<Long>,
    val playerPerspectiveIds: Set<Long>,
    val gameModeIds: Set<Long>,
    val keywordIds: Set<Long>,
    val websiteTypeIds: Set<Long>,
    val platformLogoIds: Set<Long>,
    val platformTypeIds: Set<Long>,
    val platformIds: Set<Long>,
    val companyIds: Set<Long>,
)

private data class ServiceEtlValidationResult(
    val mismatchCount: Int,
    val sampleMismatches: List<ServiceEtlMismatchSample>,
)

private data class ServiceEtlMismatchSample(
    val tableName: String,
    val mismatchType: String,
    val expectedValue: String?,
    val actualValue: String?,
    val detailsJson: String?,
)

private class ServiceEtlValidationException(
    val mismatchCount: Int,
    val mismatchSamples: List<ServiceEtlMismatchSample>,
) : RuntimeException("service ETL validation failed with $mismatchCount mismatches")

private class ValidationAccumulator {
    private val sampleMismatches = mutableListOf<ServiceEtlMismatchSample>()
    private var mismatchCount = 0

    fun <T, K> collect(
        tableName: String,
        expectedRows: List<T>,
        actualRows: List<T>,
        keySelector: (T) -> K,
    ) {
        val expectedByKey = expectedRows.associateBy(keySelector)
        val actualByKey = actualRows.associateBy(keySelector)
        val keys = linkedSetOf<K>().apply {
            addAll(expectedByKey.keys)
            addAll(actualByKey.keys)
        }

        keys.forEach { key ->
            val expectedRow = expectedByKey[key]
            val actualRow = actualByKey[key]
            when {
                expectedRow == null -> record(tableName, "unexpected_row", null, actualRow.toString(), key)
                actualRow == null -> record(tableName, "missing_row", expectedRow.toString(), null, key)
                expectedRow != actualRow -> record(tableName, "different_value", expectedRow.toString(), actualRow.toString(), key)
            }
        }
    }

    private fun record(
        tableName: String,
        mismatchType: String,
        expectedValue: String?,
        actualValue: String?,
        key: Any?,
    ) {
        mismatchCount += 1
        if (sampleMismatches.size >= VALIDATION_SAMPLE_LIMIT) {
            return
        }
        sampleMismatches += ServiceEtlMismatchSample(
            tableName = tableName,
            mismatchType = mismatchType,
            expectedValue = expectedValue,
            actualValue = actualValue,
            detailsJson = key?.let { """{"key":"${it.toString().toJsonEscapedString()}"}""" },
        )
    }

    fun toResult() = ServiceEtlValidationResult(
        mismatchCount = mismatchCount,
        sampleMismatches = sampleMismatches.toList(),
    )
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

private fun List<CoverProjectionRow>.filterCoverRowsByGameIds(gameIds: Set<Long>): List<CoverProjectionRow> =
    filter { it.gameId in gameIds }

private fun List<ArtworkProjectionRow>.filterArtworkRowsByGameIds(gameIds: Set<Long>): List<ArtworkProjectionRow> =
    filter { it.gameId in gameIds }

private fun List<ScreenshotProjectionRow>.filterScreenshotRowsByGameIds(gameIds: Set<Long>): List<ScreenshotProjectionRow> =
    filter { it.gameId in gameIds }

private fun List<GameVideoProjectionRow>.filterGameVideoRowsByGameIds(gameIds: Set<Long>): List<GameVideoProjectionRow> =
    filter { it.gameId in gameIds }

private fun List<WebsiteProjectionRow>.filterWebsiteRowsByGameIds(gameIds: Set<Long>): List<WebsiteProjectionRow> =
    filter { it.gameId in gameIds }

private fun List<AlternativeNameProjectionRow>.filterAlternativeNameRowsByGameIds(gameIds: Set<Long>): List<AlternativeNameProjectionRow> =
    filter { it.gameId in gameIds }

private fun PreparedSlice2Inputs.idSets() = PreparedSlice2IdSets(
    gameStatusIds = idsOf(gameStatuses) { it.id },
    gameTypeIds = idsOf(gameTypes) { it.id },
    languageIds = idsOf(languages) { it.id },
    regionIds = idsOf(regions) { it.id },
    releaseRegionIds = idsOf(releaseRegions) { it.id },
    releaseStatusIds = idsOf(releaseStatuses) { it.id },
    genreIds = idsOf(genres) { it.id },
    themeIds = idsOf(themes) { it.id },
    playerPerspectiveIds = idsOf(playerPerspectives) { it.id },
    gameModeIds = idsOf(gameModes) { it.id },
    keywordIds = idsOf(keywords) { it.id },
    websiteTypeIds = idsOf(websiteTypes) { it.id },
    platformLogoIds = idsOf(platformLogos) { it.id },
    platformTypeIds = idsOf(platformTypes) { it.id },
    platformIds = idsOf(platforms) { it.id },
    companyIds = idsOf(companies) { it.id },
)

private fun <T> sortRowsById(rows: List<T>, idSelector: (T) -> Long): List<T> =
    rows.sortedBy(idSelector)

private fun <T> idsOf(rows: List<T>, idSelector: (T) -> Long): Set<Long> =
    rows.mapTo(linkedSetOf(), idSelector)

private fun String.toJsonEscapedString(): String =
    buildString(length) {
        this@toJsonEscapedString.forEach { ch ->
            when (ch) {
                '\\' -> append("\\\\")
                '"' -> append("\\\"")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                else -> append(ch)
            }
        }
    }
