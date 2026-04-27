package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.AlternativeNameProjectionRow
import com.projectgc.calendar.repository.etl.ArtworkProjectionRow
import com.projectgc.calendar.repository.etl.CoverProjectionRow
import com.projectgc.calendar.repository.etl.GameProjectionRow
import com.projectgc.calendar.repository.etl.GameReleaseProjectionRow
import com.projectgc.calendar.repository.etl.GameLocalizationProjectionRow
import com.projectgc.calendar.repository.etl.GameVideoProjectionRow
import com.projectgc.calendar.repository.etl.IngestEtlReadJdbcRepository
import com.projectgc.calendar.repository.etl.NamedDimensionRow
import com.projectgc.calendar.repository.etl.ScreenshotProjectionRow
import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import com.projectgc.calendar.repository.etl.ServiceEtlMismatchLogEntry
import com.projectgc.calendar.repository.etl.ServiceEtlSourceLogEntry
import com.projectgc.calendar.repository.etl.ServiceEtlTableSyncResult
import com.projectgc.calendar.repository.etl.WebsiteProjectionRow
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.anyLong
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.reset
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.AbstractPlatformTransactionManager
import org.springframework.transaction.support.DefaultTransactionStatus
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ServiceEtlServiceTest {
    private val ingestRepository = mock(IngestEtlReadJdbcRepository::class.java)
    private val repository = mock(ServiceEtlJdbcRepository::class.java)
    private val affectedGameIdCalculator = mock(AffectedGameIdCalculator::class.java)
    private val service = newService(NoOpTransactionManager())
    private val sourceLogs = mutableListOf<ServiceEtlSourceLogEntry>()
    private val mismatchLogs = mutableListOf<ServiceEtlMismatchLogEntry>()
    private val finishEntries = mutableListOf<FinishEntry>()

    @BeforeEach
    fun setUp() {
        reset(ingestRepository, repository, affectedGameIdCalculator)
        sourceLogs.clear()
        mismatchLogs.clear()
        finishEntries.clear()
        doAnswer { invocation ->
            sourceLogs += invocation.arguments[0] as ServiceEtlSourceLogEntry
            null
        }.`when`(repository).insertSourceLog(anyObject(ServiceEtlSourceLogEntry::class.java))
        doAnswer { invocation ->
            mismatchLogs += invocation.arguments[0] as List<ServiceEtlMismatchLogEntry>
            null
        }.`when`(repository).insertMismatchLogs(anyList())
        doAnswer { invocation ->
            finishEntries += FinishEntry(
                status = invocation.arguments[2] as String,
                mismatchCount = invocation.arguments[3] as Int,
                errorMessage = invocation.arguments[4] as String?,
            )
            null
        }.`when`(repository).finishRunLog(
            anyObject(UUID::class.java),
            anyObject(Instant::class.java),
            anyObject(String::class.java),
            org.mockito.Mockito.anyInt(),
            nullableObject(String::class.java),
        )
        stubEmptySlice2Syncs()
        stubSlice7ValidationPass(emptyPreparedAffectedGameInputs())
        `when`(affectedGameIdCalculator.prepare(anyLong())).thenReturn(emptyPreparedAffectedGameInputs())
        `when`(affectedGameIdCalculator.calculate(anyObject(PreparedAffectedGameIdInputs::class.java)))
            .thenReturn(emptySlice7CalculationResult())
    }

    @Test
    fun `syncs slice2 sources, validates slice7 state, and completes`() {
        val preparedInputs = preparedAffectedGameInputs(linkedSetOf(101L, 102L))
        stubSlice7ValidationPass(preparedInputs)
        `when`(affectedGameIdCalculator.prepare(anyLong())).thenReturn(preparedInputs)
        `when`(affectedGameIdCalculator.calculate(anyObject(PreparedAffectedGameIdInputs::class.java))).thenReturn(
            slice7CalculationResult(
                perTableGameIds = mapOf(
                    "game" to setOf(101L, 102L),
                    "release_date" to setOf(101L),
                    "cover" to setOf(102L),
                ),
            )
        )

        service.run(UUID.randomUUID(), ServiceEtlTrigger.manual())

        verify(repository).syncPlatforms(anyList(), anyLongSet(), anyLongSet())
        verify(repository).rebuildCoreGameProjections(
            anyList(),
            anyList(),
            anyList(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
        )
        verify(repository).rebuildGameDependentBridgeProjections(
            anyLongSet(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
        )
        verify(repository).rebuildGameMediaProjections(
            anyLongSet(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyLongSet(),
            anyLongMap(),
            anyLongSet(),
        )
        assertTrue(sourceLogs.any { it.tableName == "game" && it.note!!.contains("slice7 projections rebuilt and validated") })
        assertEquals(emptyList(), mismatchLogs)
        assertEquals(listOf(FinishEntry(status = "completed", mismatchCount = 0, errorMessage = null)), finishEntries)
    }

    @Test
    fun `rebuilds shared-dimension deletion fallout even when calculator affected set is empty`() {
        val preparedInputs = preparedAffectedGameInputs(linkedSetOf(11L))
        val staleGameRow = preparedInputs.gameRows.single().copy(statusId = 999L)
        val rebuiltGameRows = mutableListOf<List<GameProjectionRow>>()

        stubSlice7ValidationPass(preparedInputs)
        `when`(affectedGameIdCalculator.prepare(anyLong())).thenReturn(preparedInputs)
        `when`(affectedGameIdCalculator.calculate(anyObject(PreparedAffectedGameIdInputs::class.java)))
            .thenReturn(emptySlice7CalculationResult())
        `when`(repository.loadCurrentGameProjectionRows()).thenReturn(
            listOf(staleGameRow),
            preparedInputs.gameRows,
        )
        doAnswer { invocation ->
            rebuiltGameRows += invocation.arguments[0] as List<GameProjectionRow>
            null
        }.`when`(repository).rebuildCoreGameProjections(
            anyList(),
            anyList(),
            anyList(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
        )

        service.run(UUID.randomUUID(), ServiceEtlTrigger.manual())

        assertEquals(listOf(listOf(preparedInputs.gameRows.single())), rebuiltGameRows)
        assertEquals("completed", finishEntries.single().status)
    }

    @Test
    fun `loads ingest snapshots before service transaction begins`() {
        val recordingTransactionManager = RecordingTransactionManager()
        val recordingService = newService(recordingTransactionManager)
        val events = recordingTransactionManager.events
        val preparedInputs = preparedAffectedGameInputs(linkedSetOf(11L))
        stubSlice7ValidationPass(preparedInputs)

        doAnswer {
            events += "ingest-load-game-statuses"
            emptyList<NamedDimensionRow>()
        }.`when`(ingestRepository).loadGameStatuses()
        doAnswer {
            events += "calculator-prepare"
            preparedInputs
        }.`when`(affectedGameIdCalculator).prepare(anyLong())
        doAnswer {
            events += "service-sync-game-statuses"
            ServiceEtlTableSyncResult(processedRows = 0, nextCursor = null)
        }.`when`(repository).syncGameStatuses(anyList())
        doAnswer {
            events += "calculator-calculate"
            emptySlice7CalculationResult()
        }.`when`(affectedGameIdCalculator).calculate(anyObject(PreparedAffectedGameIdInputs::class.java))

        recordingService.run(UUID.randomUUID(), ServiceEtlTrigger.manual())

        val transactionBeginIndex = events.indexOf("tx-begin")
        assertTrue(transactionBeginIndex > 0)
        assertTrue(events.indexOf("ingest-load-game-statuses") < transactionBeginIndex)
        assertTrue(events.indexOf("calculator-prepare") < transactionBeginIndex)
        assertTrue(events.indexOf("service-sync-game-statuses") > transactionBeginIndex)
        assertTrue(events.indexOf("calculator-calculate") > transactionBeginIndex)

        val order = inOrder(ingestRepository, affectedGameIdCalculator, repository)
        order.verify(ingestRepository).loadGameStatuses()
        order.verify(affectedGameIdCalculator).prepare(anyLong())
        order.verify(repository).syncGameStatuses(anyList())
        order.verify(affectedGameIdCalculator).calculate(anyObject(PreparedAffectedGameIdInputs::class.java))
    }

    @Test
    fun `retries once when validation fails and then completes`() {
        val preparedInputs = preparedAffectedGameInputs(linkedSetOf(1L))
        val expectedGameRow = preparedInputs.gameRows.single()
        stubSlice7ValidationPass(preparedInputs)
        `when`(repository.loadCurrentGameProjectionRows()).thenReturn(
            listOf(expectedGameRow),
            listOf(expectedGameRow.copy(name = "wrong-name")),
            listOf(expectedGameRow),
            listOf(expectedGameRow),
        )
        `when`(affectedGameIdCalculator.prepare(anyLong())).thenReturn(preparedInputs)
        `when`(affectedGameIdCalculator.calculate(anyObject(PreparedAffectedGameIdInputs::class.java))).thenReturn(
            slice7CalculationResult(perTableGameIds = mapOf("game" to setOf(1L)))
        )

        service.run(UUID.randomUUID(), ServiceEtlTrigger.manual())

        verify(affectedGameIdCalculator, times(2)).calculate(anyObject(PreparedAffectedGameIdInputs::class.java))
        verify(repository, times(2)).rebuildCoreGameProjections(
            anyList(),
            anyList(),
            anyList(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
            anyLongSet(),
        )
        assertEquals(emptyList(), mismatchLogs)
        assertEquals("completed", finishEntries.single().status)
    }

    @Test
    fun `persists mismatch samples after second validation failure`() {
        val preparedInputs = preparedAffectedGameInputs(linkedSetOf(1L))
        stubSlice7ValidationPass(preparedInputs)
        `when`(repository.loadCurrentGameProjectionRows()).thenReturn(
            listOf(preparedInputs.gameRows.single().copy(name = "wrong-name"))
        )
        `when`(affectedGameIdCalculator.prepare(anyLong())).thenReturn(preparedInputs)
        `when`(affectedGameIdCalculator.calculate(anyObject(PreparedAffectedGameIdInputs::class.java))).thenReturn(
            slice7CalculationResult(perTableGameIds = mapOf("game" to setOf(1L)))
        )

        val error = assertFailsWith<RuntimeException> {
            service.run(UUID.randomUUID(), ServiceEtlTrigger.manual())
        }

        assertTrue(error.message!!.contains("validation failed"))
        verify(affectedGameIdCalculator, times(2)).calculate(anyObject(PreparedAffectedGameIdInputs::class.java))
        assertTrue(mismatchLogs.isNotEmpty())
        assertTrue(mismatchLogs.first().tableName == "service.game")
        assertEquals("failed", finishEntries.single().status)
        assertTrue(finishEntries.single().mismatchCount > 0)
    }

    @Test
    fun `retries once when the first slice2 source throws and then fails`() {
        `when`(repository.syncGameStatuses(anyList())).thenThrow(RuntimeException("game_status sync failed"))

        assertFailsWith<RuntimeException> {
            service.run(UUID.randomUUID(), ServiceEtlTrigger.manual())
        }

        verify(repository, times(2)).syncGameStatuses(anyList())
        assertEquals(emptyList(), mismatchLogs)
        assertEquals("failed", finishEntries.single().status)
        assertTrue(finishEntries.single().errorMessage!!.contains("game_status sync failed"))
    }

    private fun stubEmptySlice2Syncs() {
        val emptyCursorResult = ServiceEtlTableSyncResult(processedRows = 0, nextCursor = null)
        val emptyPlatformLogoResult = ServiceEtlTableSyncResult(
            processedRows = 0,
            nextCursor = null,
            note = "diff-based upsert: ingest.platform_logo has no updated_at cursor",
        )

        `when`(ingestRepository.loadGameStatuses()).thenReturn(emptyList<NamedDimensionRow>())
        `when`(ingestRepository.loadGameTypes()).thenReturn(emptyList())
        `when`(ingestRepository.loadLanguages()).thenReturn(emptyList())
        `when`(ingestRepository.loadRegions()).thenReturn(emptyList())
        `when`(ingestRepository.loadReleaseRegions()).thenReturn(emptyList())
        `when`(ingestRepository.loadReleaseStatuses()).thenReturn(emptyList())
        `when`(ingestRepository.loadGenres()).thenReturn(emptyList())
        `when`(ingestRepository.loadThemes()).thenReturn(emptyList())
        `when`(ingestRepository.loadPlayerPerspectives()).thenReturn(emptyList())
        `when`(ingestRepository.loadGameModes()).thenReturn(emptyList())
        `when`(ingestRepository.loadKeywords()).thenReturn(emptyList())
        `when`(ingestRepository.loadLanguageSupportTypes()).thenReturn(emptyList())
        `when`(ingestRepository.loadWebsiteTypes()).thenReturn(emptyList())
        `when`(ingestRepository.loadPlatformLogos()).thenReturn(emptyList())
        `when`(ingestRepository.loadPlatformTypes()).thenReturn(emptyList())
        `when`(ingestRepository.loadPlatforms()).thenReturn(emptyList())
        `when`(ingestRepository.loadCompanies()).thenReturn(emptyList())

        `when`(repository.syncGameStatuses(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncGameTypes(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncLanguages(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncRegions(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncReleaseRegions(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncReleaseStatuses(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncGenres(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncThemes(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncPlayerPerspectives(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncGameModes(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncKeywords(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncLanguageSupportTypes(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncWebsiteTypes(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncPlatformLogos(anyList())).thenReturn(emptyPlatformLogoResult)
        `when`(repository.syncPlatformTypes(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncPlatforms(anyList(), anyLongSet(), anyLongSet())).thenReturn(emptyCursorResult)
        `when`(repository.syncCompanies(anyList())).thenReturn(emptyCursorResult)
    }

    private fun stubSlice7ValidationPass(preparedInputs: PreparedAffectedGameIdInputs) {
        val allGameIds = preparedInputs.allGameIds
        `when`(repository.loadIds(anyObject(String::class.java))).thenAnswer { invocation ->
            when (invocation.arguments[0] as String) {
                "service.game" -> allGameIds
                else -> emptySet<Long>()
            }
        }
        `when`(repository.loadCurrentNamedDimensionRows(anyObject(String::class.java), anyObject(String::class.java)))
            .thenReturn(emptyList())
        `when`(repository.loadCurrentLanguages()).thenReturn(emptyList())
        `when`(repository.loadCurrentRegions()).thenReturn(emptyList())
        `when`(repository.loadCurrentReleaseStatuses()).thenReturn(emptyList())
        `when`(repository.loadCurrentPlatformLogos()).thenReturn(emptyList())
        `when`(repository.loadCurrentPlatforms()).thenReturn(emptyList())
        `when`(repository.loadCurrentCompanies()).thenReturn(emptyList())
        `when`(repository.loadCurrentGameProjectionRows()).thenReturn(preparedInputs.gameRows)
        `when`(repository.loadCurrentGameLocalizationProjectionRows()).thenReturn(preparedInputs.gameLocalizationRows)
        `when`(repository.loadCurrentGameReleaseProjectionRows()).thenReturn(preparedInputs.gameReleaseRows)
        `when`(repository.loadCurrentGameLanguageProjectionRows()).thenReturn(emptyList())
        `when`(repository.loadCurrentGameDimensionProjectionRows(anyObject(String::class.java), anyObject(String::class.java)))
            .thenReturn(emptyList())
        `when`(repository.loadCurrentGameCompanyProjectionRows()).thenReturn(emptyList())
        `when`(repository.loadCurrentGameRelationProjectionRows()).thenReturn(emptyList())
        `when`(repository.loadCurrentCoverProjectionRows()).thenReturn(preparedInputs.coverRows)
        `when`(repository.loadCurrentArtworkProjectionRows()).thenReturn(preparedInputs.artworkRows)
        `when`(repository.loadCurrentScreenshotProjectionRows()).thenReturn(preparedInputs.screenshotRows)
        `when`(repository.loadCurrentGameVideoProjectionRows()).thenReturn(preparedInputs.gameVideoRows)
        `when`(repository.loadCurrentWebsiteProjectionRows()).thenReturn(preparedInputs.websiteRows)
        `when`(repository.loadCurrentAlternativeNameProjectionRows()).thenReturn(preparedInputs.alternativeNameRows)
    }

    private fun emptyPreparedAffectedGameInputs(): PreparedAffectedGameIdInputs =
        PreparedAffectedGameIdInputs(
            allGameIds = emptySet(),
            gameRows = emptyList(),
            gameReleaseRows = emptyList(),
            gameLocalizationRows = emptyList(),
            gameLanguageRows = emptyList(),
            gameGenreRows = emptyList(),
            gameThemeRows = emptyList(),
            gamePlayerPerspectiveRows = emptyList(),
            gameModeRows = emptyList(),
            gameKeywordRows = emptyList(),
            gameCompanyRows = emptyList(),
            gameRelationRows = emptyList(),
            coverRows = emptyList(),
            artworkRows = emptyList(),
            screenshotRows = emptyList(),
            gameVideoRows = emptyList(),
            websiteRows = emptyList(),
            alternativeNameRows = emptyList(),
        )

    private fun preparedAffectedGameInputs(gameIds: Set<Long>): PreparedAffectedGameIdInputs =
        PreparedAffectedGameIdInputs(
            allGameIds = gameIds,
            gameRows = gameIds.map(::gameRow),
            gameReleaseRows = gameIds.map(::releaseRow),
            gameLocalizationRows = gameIds.map(::localizationRow),
            gameLanguageRows = emptyList(),
            gameGenreRows = emptyList(),
            gameThemeRows = emptyList(),
            gamePlayerPerspectiveRows = emptyList(),
            gameModeRows = emptyList(),
            gameKeywordRows = emptyList(),
            gameCompanyRows = emptyList(),
            gameRelationRows = emptyList(),
            coverRows = gameIds.map(::coverRow),
            artworkRows = gameIds.map(::artworkRow),
            screenshotRows = gameIds.map(::screenshotRow),
            gameVideoRows = gameIds.map(::gameVideoRow),
            websiteRows = gameIds.map(::websiteRow),
            alternativeNameRows = gameIds.map(::alternativeNameRow),
        )

    private fun emptySlice7CalculationResult(): AffectedGameIdCalculationResult =
        slice7CalculationResult(emptyMap())

    private fun slice7CalculationResult(perTableGameIds: Map<String, Set<Long>>): AffectedGameIdCalculationResult {
        val sourceTables = listOf(
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
        val sourceResults = sourceTables.map { tableName ->
            AffectedGameIdSourceResult(
                tableName = tableName,
                cursorFrom = null,
                cursorTo = null,
                affectedGameIds = perTableGameIds[tableName].orEmpty(),
                note = "slice7 projection diff test note",
                materializedInCurrentSlice = true,
                advanceCursor = false,
            )
        }
        val affectedGameIds = linkedSetOf<Long>()
        sourceResults.forEach { affectedGameIds += it.affectedGameIds }
        return AffectedGameIdCalculationResult(
            affectedGameIds = affectedGameIds,
            sourceResults = sourceResults,
        )
    }

    private fun gameRow(id: Long) = GameProjectionRow(
        id = id,
        slug = null,
        name = "game-$id",
        summary = null,
        storyline = null,
        firstReleaseDateEpochSecond = null,
        statusId = null,
        typeId = null,
        sourceUpdatedAtEpochSecond = null,
        tags = null,
    )

    private fun releaseRow(gameId: Long) = GameReleaseProjectionRow(
        id = gameId,
        gameId = gameId,
        platformId = null,
        regionId = null,
        statusId = null,
        releaseDateEpochSecond = null,
        year = null,
        month = null,
        dateHuman = null,
    )

    private fun localizationRow(gameId: Long) = GameLocalizationProjectionRow(
        id = gameId,
        gameId = gameId,
        regionId = null,
        name = "loc-$gameId",
    )

    private fun coverRow(gameId: Long) = CoverProjectionRow(
        id = gameId + 1000,
        gameId = gameId,
        gameLocalizationId = gameId,
        imageId = "cover-$gameId",
        url = "https://example.com/cover-$gameId",
        isMain = true,
    )

    private fun artworkRow(gameId: Long) = ArtworkProjectionRow(
        id = gameId + 2000,
        gameId = gameId,
        imageId = "artwork-$gameId",
        url = "https://example.com/artwork-$gameId",
    )

    private fun screenshotRow(gameId: Long) = ScreenshotProjectionRow(
        id = gameId + 3000,
        gameId = gameId,
        imageId = "screenshot-$gameId",
        url = "https://example.com/screenshot-$gameId",
    )

    private fun gameVideoRow(gameId: Long) = GameVideoProjectionRow(
        id = gameId + 4000,
        gameId = gameId,
        name = "video-$gameId",
        videoId = "video-$gameId",
    )

    private fun websiteRow(gameId: Long) = WebsiteProjectionRow(
        id = gameId + 5000,
        gameId = gameId,
        typeId = null,
        url = "https://example.com/site-$gameId",
        isTrusted = true,
    )

    private fun alternativeNameRow(gameId: Long) = AlternativeNameProjectionRow(
        id = gameId + 6000,
        gameId = gameId,
        name = "alt-$gameId",
        comment = "comment-$gameId",
    )

    private fun newService(transactionManager: AbstractPlatformTransactionManager): ServiceEtlService =
        ServiceEtlService(
            ingestEtlReadJdbcRepository = ingestRepository,
            serviceEtlJdbcRepository = repository,
            affectedGameIdCalculator = affectedGameIdCalculator,
            transactionTemplate = TransactionTemplate(transactionManager),
        )

    @Suppress("UNCHECKED_CAST")
    private fun <T> anyObject(type: Class<T>): T {
        org.mockito.Mockito.any(type)
        return null as T
    }

    @Suppress("UNCHECKED_CAST")
    private fun anyLongSet(): Set<Long> {
        org.mockito.ArgumentMatchers.anySet<Long>()
        return emptySet()
    }

    @Suppress("UNCHECKED_CAST")
    private fun anyLongMap(): Map<Long, Long> {
        org.mockito.ArgumentMatchers.anyMap<Long, Long>()
        return emptyMap()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> anyList(): List<T> {
        org.mockito.ArgumentMatchers.anyList<T>()
        return emptyList()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> nullableObject(type: Class<T>): T? {
        org.mockito.ArgumentMatchers.nullable(type)
        return null
    }

    private data class FinishEntry(
        val status: String,
        val mismatchCount: Int,
        val errorMessage: String?,
    )

    private class NoOpTransactionManager : AbstractPlatformTransactionManager() {
        override fun doGetTransaction(): Any = Any()

        override fun doBegin(transaction: Any, definition: TransactionDefinition) = Unit

        override fun doCommit(status: DefaultTransactionStatus) = Unit

        override fun doRollback(status: DefaultTransactionStatus) = Unit
    }

    private class RecordingTransactionManager : AbstractPlatformTransactionManager() {
        val events = mutableListOf<String>()

        override fun doGetTransaction(): Any = Any()

        override fun doBegin(transaction: Any, definition: TransactionDefinition) {
            events += "tx-begin"
        }

        override fun doCommit(status: DefaultTransactionStatus) {
            events += "tx-commit"
        }

        override fun doRollback(status: DefaultTransactionStatus) {
            events += "tx-rollback"
        }
    }
}
