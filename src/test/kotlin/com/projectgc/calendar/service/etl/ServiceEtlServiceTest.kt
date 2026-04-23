package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.AlternativeNameProjectionRow
import com.projectgc.calendar.repository.etl.ArtworkProjectionRow
import com.projectgc.calendar.repository.etl.CoverProjectionRow
import com.projectgc.calendar.repository.etl.GameCompanyProjectionRow
import com.projectgc.calendar.repository.etl.GameVideoProjectionRow
import com.projectgc.calendar.repository.etl.GameLocalizationProjectionRow
import com.projectgc.calendar.repository.etl.GameProjectionRow
import com.projectgc.calendar.repository.etl.GameReleaseProjectionRow
import com.projectgc.calendar.repository.etl.IngestEtlReadJdbcRepository
import com.projectgc.calendar.repository.etl.NamedDimensionRow
import com.projectgc.calendar.repository.etl.ScreenshotProjectionRow
import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
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
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ServiceEtlServiceTest {
    companion object {
        private val SLICE6_SOURCE_TABLES = listOf(
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

    private val ingestRepository = mock(IngestEtlReadJdbcRepository::class.java)
    private val repository = mock(ServiceEtlJdbcRepository::class.java)
    private val affectedGameIdCalculator = mock(AffectedGameIdCalculator::class.java)
    private val service = newService(NoOpTransactionManager())
    private val sourceLogs = mutableListOf<ServiceEtlSourceLogEntry>()
    private val cursorWrites = mutableListOf<Pair<String, Long>>()
    private val finishedStatuses = mutableListOf<String>()

    @BeforeEach
    fun setUp() {
        reset(ingestRepository, repository, affectedGameIdCalculator)
        sourceLogs.clear()
        cursorWrites.clear()
        finishedStatuses.clear()
        `when`(repository.findCursor(anyObject(String::class.java))).thenReturn(null)
        `when`(affectedGameIdCalculator.prepare(anyLong())).thenReturn(emptyPreparedAffectedGameInputs())
        `when`(affectedGameIdCalculator.calculate(anyObject(PreparedAffectedGameIdInputs::class.java)))
            .thenReturn(emptySlice6CalculationResult())
        doAnswer { invocation ->
            sourceLogs += invocation.arguments[0] as ServiceEtlSourceLogEntry
            null
        }.`when`(repository).insertSourceLog(anyObject(ServiceEtlSourceLogEntry::class.java))
        doAnswer { invocation ->
            cursorWrites += (invocation.arguments[0] as String) to (invocation.arguments[1] as Long)
            null
        }.`when`(repository).upsertCursor(
            anyObject(String::class.java),
            org.mockito.Mockito.anyLong(),
            anyObject(Instant::class.java),
        )
        doAnswer { invocation ->
            finishedStatuses += invocation.arguments[2] as String
            null
        }.`when`(repository).finishRunLog(
            anyObject(UUID::class.java),
            anyObject(Instant::class.java),
            anyObject(String::class.java),
            org.mockito.Mockito.anyInt(),
            nullableObject(String::class.java),
        )
        stubEmptySlice2Syncs()
    }

    @Test
    fun `syncs prepared slice2 sources and rebuilds slice6 projections for all affected sources`() {
        `when`(repository.syncGameStatuses(anyList()))
            .thenReturn(ServiceEtlTableSyncResult(processedRows = 2, nextCursor = null))
        `when`(repository.syncPlatformLogos(anyList()))
            .thenReturn(
                ServiceEtlTableSyncResult(
                    processedRows = 1,
                    nextCursor = null,
                    note = "diff-based upsert: ingest.platform_logo has no updated_at cursor",
                )
            )
        `when`(repository.syncCompanies(anyList()))
            .thenReturn(ServiceEtlTableSyncResult(processedRows = 3, nextCursor = null))
        `when`(affectedGameIdCalculator.prepare(anyLong())).thenReturn(
            preparedAffectedGameInputs(gameIds = linkedSetOf(101L, 102L, 103L, 104L, 105L, 106L))
        )
        `when`(affectedGameIdCalculator.calculate(anyObject(PreparedAffectedGameIdInputs::class.java))).thenReturn(
            slice6CalculationResult(
                perTableGameIds = mapOf(
                    "game" to setOf(101L, 102L, 103L),
                    "release_date" to setOf(103L, 104L),
                    "involved_company" to setOf(105L),
                    "language_support" to setOf(106L),
                    "cover" to setOf(101L, 107L),
                    "website" to setOf(102L),
                ),
                noteByTable = mapOf(
                    "game" to "slice6 projection diff",
                    "cover" to "slice6 cover projection diff",
                ),
            )
        )

        val runId = UUID.randomUUID()
        service.run(runId, ServiceEtlTrigger.manual())

        verify(affectedGameIdCalculator).prepare(anyLong())
        verify(affectedGameIdCalculator).calculate(anyObject(PreparedAffectedGameIdInputs::class.java))
        verify(ingestRepository, never()).loadGameProjectionRows(anyLongSet())
        verify(ingestRepository, never()).loadGameLocalizationProjectionRows(anyLongSet())
        verify(ingestRepository, never()).loadGameReleaseProjectionRows(anyLongSet())
        verify(repository).rebuildCoreGameProjections(anyList(), anyList(), anyList())
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
        )
        verify(repository).rebuildGameMediaProjections(
            anyLongSet(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
        )

        assertEquals(28, sourceLogs.size)
        val logsByTable = sourceLogs.associateBy { it.tableName }

        val gameStatusLog = logsByTable.getValue("game_status")
        assertEquals("completed", gameStatusLog.status)
        assertEquals(2, gameStatusLog.processedRows)
        assertNull(gameStatusLog.cursorFrom)
        assertNull(gameStatusLog.cursorTo)

        val platformLogoLog = logsByTable.getValue("platform_logo")
        assertEquals("completed", platformLogoLog.status)
        assertEquals(1, platformLogoLog.processedRows)
        assertNull(platformLogoLog.cursorFrom)
        assertNull(platformLogoLog.cursorTo)
        assertNotNull(platformLogoLog.note)

        val gameLog = logsByTable.getValue("game")
        assertEquals("completed", gameLog.status)
        assertEquals(3, gameLog.processedRows)
        assertNull(gameLog.cursorFrom)
        assertNull(gameLog.cursorTo)
        assertNotNull(gameLog.note)
        assertTrue(gameLog.note!!.contains("slice6 projections rebuilt"))
        assertTrue(!gameLog.note!!.contains("dry-run"))

        val releaseDateLog = logsByTable.getValue("release_date")
        assertEquals("completed", releaseDateLog.status)
        assertEquals(2, releaseDateLog.processedRows)
        assertNull(releaseDateLog.cursorFrom)
        assertNull(releaseDateLog.cursorTo)
        assertTrue(releaseDateLog.note!!.contains("slice6 projections rebuilt"))

        val coverLog = logsByTable.getValue("cover")
        assertEquals("completed", coverLog.status)
        assertEquals(2, coverLog.processedRows)
        assertNull(coverLog.cursorFrom)
        assertNull(coverLog.cursorTo)
        assertTrue(coverLog.note!!.contains("slice6 projections rebuilt"))

        val websiteLog = logsByTable.getValue("website")
        assertEquals("completed", websiteLog.status)
        assertEquals(1, websiteLog.processedRows)
        assertNull(websiteLog.cursorFrom)
        assertNull(websiteLog.cursorTo)
        assertTrue(websiteLog.note!!.contains("slice6 projections rebuilt"))

        assertTrue(sourceLogs.none { it.status == "skipped" })
        assertTrue(sourceLogs.none { it.note?.contains("dry-run") == true })
        assertEquals(emptyList(), cursorWrites)
        assertEquals(listOf("completed"), finishedStatuses)
    }

    @Test
    fun `loads ingest snapshots before service transaction begins`() {
        val recordingTransactionManager = RecordingTransactionManager()
        val recordingService = newService(recordingTransactionManager)
        val events = recordingTransactionManager.events
        val preparedInputs = preparedAffectedGameInputs(gameIds = linkedSetOf(11L))

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
            emptySlice6CalculationResult()
        }.`when`(affectedGameIdCalculator).calculate(anyObject(PreparedAffectedGameIdInputs::class.java))
        doAnswer {
            events += "service-rebuild-core"
            null
        }.`when`(repository).rebuildCoreGameProjections(anyList(), anyList(), anyList())

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
    fun `keeps slice2 and slice6 projection sources cursorless after empty rebuild`() {
        `when`(affectedGameIdCalculator.prepare(anyLong())).thenReturn(emptyPreparedAffectedGameInputs())
        `when`(affectedGameIdCalculator.calculate(anyObject(PreparedAffectedGameIdInputs::class.java))).thenReturn(
            slice6CalculationResult(perTableGameIds = emptyMap())
        )

        val runId = UUID.randomUUID()
        service.run(runId, ServiceEtlTrigger.manual())

        assertEquals(28, sourceLogs.size)
        val gameStatusLog = sourceLogs.first { it.tableName == "game_status" }
        val gameLog = sourceLogs.first { it.tableName == "game" }
        val websiteLog = sourceLogs.first { it.tableName == "website" }

        assertEquals("completed", gameStatusLog.status)
        assertEquals(0, gameStatusLog.processedRows)
        assertNull(gameStatusLog.cursorFrom)
        assertNull(gameStatusLog.cursorTo)

        assertEquals("completed", gameLog.status)
        assertEquals(0, gameLog.processedRows)
        assertNull(gameLog.cursorFrom)
        assertNull(gameLog.cursorTo)
        assertTrue(gameLog.note!!.contains("slice6 projections rebuilt"))

        assertEquals("completed", websiteLog.status)
        assertEquals(0, websiteLog.processedRows)
        assertNull(websiteLog.cursorFrom)
        assertNull(websiteLog.cursorTo)
        assertTrue(websiteLog.note!!.contains("slice6 projections rebuilt"))

        verify(repository).rebuildCoreGameProjections(anyList(), anyList(), anyList())
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
        )
        verify(repository).rebuildGameMediaProjections(
            anyLongSet(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
        )
        assertEquals(emptyList(), cursorWrites)
        verify(repository, never()).findCursor("game_status")
        verify(repository, never()).findCursor("platform_logo")
        assertEquals(listOf("completed"), finishedStatuses)
    }

    @Test
    fun `marks run failed when the first slice2 source throws`() {
        `when`(repository.syncGameStatuses(anyList())).thenThrow(RuntimeException("game_status sync failed"))

        val runId = UUID.randomUUID()

        assertFailsWith<RuntimeException> {
            service.run(runId, ServiceEtlTrigger.manual())
        }

        assertEquals(listOf("failed"), finishedStatuses)
        assertEquals(emptyList(), cursorWrites)
        assertEquals(emptyList(), sourceLogs)
        verify(affectedGameIdCalculator).prepare(anyLong())
        verify(affectedGameIdCalculator, never()).calculate(anyObject(PreparedAffectedGameIdInputs::class.java))
        verify(repository, never()).rebuildCoreGameProjections(anyList(), anyList(), anyList())
        verify(repository, never()).rebuildGameDependentBridgeProjections(
            anyLongSet(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
        )
        verify(repository, never()).rebuildGameMediaProjections(
            anyLongSet(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
        )
    }

    @Test
    fun `marks run failed when slice6 media projection rebuild throws`() {
        `when`(affectedGameIdCalculator.prepare(anyLong())).thenReturn(
            preparedAffectedGameInputs(gameIds = linkedSetOf(101L, 102L, 103L))
        )
        `when`(affectedGameIdCalculator.calculate(anyObject(PreparedAffectedGameIdInputs::class.java))).thenReturn(
            slice6CalculationResult(
                perTableGameIds = mapOf(
                    "game" to setOf(101L),
                    "release_date" to setOf(102L),
                    "involved_company" to setOf(103L),
                    "cover" to setOf(101L),
                ),
            )
        )
        doThrow(RuntimeException("media projection rebuild failed"))
            .`when`(repository)
            .rebuildGameMediaProjections(
                anyLongSet(),
                anyList(),
                anyList(),
                anyList(),
                anyList(),
                anyList(),
                anyList(),
            )

        val runId = UUID.randomUUID()

        assertFailsWith<RuntimeException> {
            service.run(runId, ServiceEtlTrigger.manual())
        }

        verify(affectedGameIdCalculator).prepare(anyLong())
        verify(affectedGameIdCalculator).calculate(anyObject(PreparedAffectedGameIdInputs::class.java))
        verify(repository).rebuildCoreGameProjections(anyList(), anyList(), anyList())
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
        )
        verify(repository).rebuildGameMediaProjections(
            anyLongSet(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
            anyList(),
        )
        assertEquals(listOf("failed"), finishedStatuses)
        assertEquals(emptyList(), cursorWrites)
        assertTrue(sourceLogs.none { it.tableName in SLICE6_SOURCE_TABLES })
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
        `when`(repository.syncPlatforms(anyList())).thenReturn(emptyCursorResult)
        `when`(repository.syncCompanies(anyList())).thenReturn(emptyCursorResult)
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
            gameCompanyRows = gameIds.map { gameId ->
                GameCompanyProjectionRow(
                    gameId = gameId,
                    companyId = gameId + 1000,
                    isDeveloper = true,
                    isPublisher = false,
                    isPorting = false,
                    isSupporting = false,
                )
            },
            gameRelationRows = emptyList(),
            coverRows = gameIds.map { gameId ->
                CoverProjectionRow(
                    id = gameId + 2000,
                    gameId = gameId,
                    gameLocalizationId = gameId,
                    imageId = "cover-$gameId",
                    url = "https://example.com/cover-$gameId",
                    isMain = true,
                )
            },
            artworkRows = gameIds.map { gameId ->
                ArtworkProjectionRow(
                    id = gameId + 3000,
                    gameId = gameId,
                    imageId = "artwork-$gameId",
                    url = "https://example.com/artwork-$gameId",
                )
            },
            screenshotRows = gameIds.map { gameId ->
                ScreenshotProjectionRow(
                    id = gameId + 4000,
                    gameId = gameId,
                    imageId = "screenshot-$gameId",
                    url = "https://example.com/screenshot-$gameId",
                )
            },
            gameVideoRows = gameIds.map { gameId ->
                GameVideoProjectionRow(
                    id = gameId + 5000,
                    gameId = gameId,
                    name = "video-$gameId",
                    videoId = "vid-$gameId",
                )
            },
            websiteRows = gameIds.map { gameId ->
                WebsiteProjectionRow(
                    id = gameId + 6000,
                    gameId = gameId,
                    typeId = null,
                    url = "https://example.com/site-$gameId",
                    isTrusted = true,
                )
            },
            alternativeNameRows = gameIds.map { gameId ->
                AlternativeNameProjectionRow(
                    id = gameId + 7000,
                    gameId = gameId,
                    name = "alt-$gameId",
                    comment = "comment-$gameId",
                )
            },
        )

    private fun emptySlice6CalculationResult(): AffectedGameIdCalculationResult =
        slice6CalculationResult(perTableGameIds = emptyMap())

    private fun slice6CalculationResult(
        perTableGameIds: Map<String, Set<Long>>,
        noteByTable: Map<String, String> = emptyMap(),
    ): AffectedGameIdCalculationResult {
        val sourceResults = SLICE6_SOURCE_TABLES.map { tableName ->
            AffectedGameIdSourceResult(
                tableName = tableName,
                cursorFrom = null,
                cursorTo = null,
                affectedGameIds = perTableGameIds[tableName].orEmpty(),
                note = noteByTable[tableName] ?: "slice6 projection diff test note",
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
    private fun <T> anyList(): List<T> {
        org.mockito.ArgumentMatchers.anyList<T>()
        return emptyList()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> nullableObject(type: Class<T>): T? {
        org.mockito.ArgumentMatchers.nullable(type)
        return null
    }

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
