package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import com.projectgc.calendar.repository.etl.ServiceEtlSourceLogEntry
import com.projectgc.calendar.repository.etl.ServiceEtlTableSyncResult
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.anyLong
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.doThrow
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
        private const val SLICE3_CURSOR_TO = 500L
        private val SLICE4_CORE_SOURCE_TABLES = setOf("game", "release_date", "game_localization")
        private val SLICE3_SOURCE_TABLES = listOf(
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

    private val repository = mock(ServiceEtlJdbcRepository::class.java)
    private val affectedGameIdCalculator = mock(AffectedGameIdCalculator::class.java)
    private val service = ServiceEtlService(
        serviceEtlJdbcRepository = repository,
        affectedGameIdCalculator = affectedGameIdCalculator,
        transactionTemplate = TransactionTemplate(NoOpTransactionManager()),
    )
    private val sourceLogs = mutableListOf<ServiceEtlSourceLogEntry>()
    private val cursorWrites = mutableListOf<Pair<String, Long>>()
    private val finishedStatuses = mutableListOf<String>()

    @BeforeEach
    fun setUp() {
        reset(repository, affectedGameIdCalculator)
        sourceLogs.clear()
        cursorWrites.clear()
        finishedStatuses.clear()
        `when`(repository.findCursor(anyObject(String::class.java))).thenReturn(null)
        `when`(affectedGameIdCalculator.calculate(anyLong())).thenReturn(emptySlice3CalculationResult())
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
    fun `syncs slice2 sources and rebuilds slice4 core projections without advancing deferred slice3 cursors`() {
        `when`(repository.syncGameStatuses())
            .thenReturn(ServiceEtlTableSyncResult(processedRows = 2, nextCursor = null))
        `when`(repository.syncPlatformLogos())
            .thenReturn(
                ServiceEtlTableSyncResult(
                    processedRows = 1,
                    nextCursor = null,
                    note = "diff-based upsert: ingest.platform_logo has no updated_at cursor",
                )
            )
        `when`(repository.syncCompanies())
            .thenReturn(ServiceEtlTableSyncResult(processedRows = 3, nextCursor = null))
        `when`(affectedGameIdCalculator.calculate(anyLong())).thenReturn(
            slice3CalculationResult(
                perTableGameIds = mapOf(
                    "game" to setOf(101L, 102L, 103L),
                    "release_date" to setOf(103L, 104L),
                    "involved_company" to setOf(105L),
                    "cover" to setOf(101L, 106L),
                ),
                cursorFromByTable = mapOf(
                    "release_date" to 120L,
                    "involved_company" to 130L,
                    "cover" to 140L,
                ),
                noteByTable = mapOf(
                    "game" to "slice3 initial full sweep",
                    "cover" to "slice3 game updated_at delta",
                ),
            )
        )

        val runId = UUID.randomUUID()
        service.run(runId, ServiceEtlTrigger.manual())

        verify(repository, never()).findCursor("game_status")
        verify(repository, never()).findCursor("keyword")
        verify(repository, never()).findCursor("platform_logo")
        verify(repository, never()).findCursor("company")
        verify(repository).rebuildCoreGameProjections(setOf(101L, 102L, 103L, 104L))

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
        assertTrue(gameLog.note!!.contains("slice4 core projection rebuilt"))
        assertTrue(!gameLog.note!!.contains("dry-run"))

        val releaseDateLog = logsByTable.getValue("release_date")
        assertEquals("completed", releaseDateLog.status)
        assertEquals(2, releaseDateLog.processedRows)
        assertNull(releaseDateLog.cursorFrom)
        assertNull(releaseDateLog.cursorTo)
        assertTrue(releaseDateLog.note!!.contains("slice4 core projection rebuilt"))

        val involvedCompanyLog = logsByTable.getValue("involved_company")
        assertEquals("completed", involvedCompanyLog.status)
        assertEquals(1, involvedCompanyLog.processedRows)
        assertEquals(130L, involvedCompanyLog.cursorFrom)
        assertEquals(SLICE3_CURSOR_TO, involvedCompanyLog.cursorTo)
        assertTrue(involvedCompanyLog.note!!.contains("deferred source dry-run"))
        assertTrue(!involvedCompanyLog.note!!.contains("core projection rebuilt"))

        val coverLog = logsByTable.getValue("cover")
        assertEquals("completed", coverLog.status)
        assertEquals(2, coverLog.processedRows)
        assertEquals(140L, coverLog.cursorFrom)
        assertEquals(SLICE3_CURSOR_TO, coverLog.cursorTo)
        assertTrue(coverLog.note!!.contains("deferred source dry-run"))
        assertTrue(!coverLog.note!!.contains("core projection rebuilt"))

        assertTrue(sourceLogs.none { it.status == "skipped" })
        assertEquals(emptyList(), cursorWrites)

        assertEquals(listOf("completed"), finishedStatuses)
    }

    @Test
    fun `keeps slice2 diff-based sources cursorless and logs deferred slice3 sources without cursor writes after empty slice4 rebuild`() {
        `when`(repository.syncGameStatuses())
            .thenReturn(ServiceEtlTableSyncResult(processedRows = 0, nextCursor = null))
        `when`(affectedGameIdCalculator.calculate(anyLong())).thenReturn(
            slice3CalculationResult(
                perTableGameIds = emptyMap(),
                cursorFromByTable = SLICE3_SOURCE_TABLES.associateWith { 200L },
                cursorTo = 300L,
            )
        )

        val runId = UUID.randomUUID()
        service.run(runId, ServiceEtlTrigger.manual())

        assertEquals(28, sourceLogs.size)
        val gameStatusLog = sourceLogs.first { it.tableName == "game_status" }
        val gameLog = sourceLogs.first { it.tableName == "game" }

        assertEquals("completed", gameStatusLog.status)
        assertEquals(0, gameStatusLog.processedRows)
        assertNull(gameStatusLog.cursorFrom)
        assertNull(gameStatusLog.cursorTo)

        assertEquals("completed", gameLog.status)
        assertEquals(0, gameLog.processedRows)
        assertNull(gameLog.cursorFrom)
        assertNull(gameLog.cursorTo)
        assertTrue(gameLog.note!!.contains("slice4 core projection rebuilt"))

        val websiteLog = sourceLogs.first { it.tableName == "website" }
        assertEquals("completed", websiteLog.status)
        assertEquals(0, websiteLog.processedRows)
        assertEquals(200L, websiteLog.cursorFrom)
        assertEquals(300L, websiteLog.cursorTo)
        assertTrue(websiteLog.note!!.contains("deferred source dry-run"))

        verify(repository).rebuildCoreGameProjections(emptySet())
        assertEquals(emptyList(), cursorWrites)
        verify(repository, never()).findCursor("game_status")
        verify(repository, never()).findCursor("platform_logo")
        assertEquals(listOf("completed"), finishedStatuses)
    }

    @Test
    fun `marks run failed when the first slice2 source throws`() {
        `when`(repository.syncGameStatuses()).thenThrow(RuntimeException("game_status sync failed"))

        val runId = UUID.randomUUID()

        assertFailsWith<RuntimeException> {
            service.run(runId, ServiceEtlTrigger.manual())
        }

        assertEquals(listOf("failed"), finishedStatuses)
        assertEquals(emptyList(), cursorWrites)
        assertEquals(emptyList(), sourceLogs)
        verify(affectedGameIdCalculator, never()).calculate(anyLong())
        verify(repository, never()).rebuildCoreGameProjections(anyLongSet())
    }

    @Test
    fun `marks run failed when slice4 core projection rebuild throws and does not advance slice3 cursors`() {
        `when`(affectedGameIdCalculator.calculate(anyLong())).thenReturn(
            slice3CalculationResult(
                perTableGameIds = mapOf("game" to setOf(101L), "release_date" to setOf(102L)),
            )
        )
        doThrow(RuntimeException("core projection rebuild failed"))
            .`when`(repository)
            .rebuildCoreGameProjections(anyLongSet())

        val runId = UUID.randomUUID()

        assertFailsWith<RuntimeException> {
            service.run(runId, ServiceEtlTrigger.manual())
        }

        verify(repository).rebuildCoreGameProjections(setOf(101L, 102L))
        assertEquals(listOf("failed"), finishedStatuses)
        assertEquals(emptyList(), cursorWrites)
        assertTrue(sourceLogs.none { it.tableName in SLICE3_SOURCE_TABLES })
    }

    private fun stubEmptySlice2Syncs() {
        val emptyCursorResult = ServiceEtlTableSyncResult(processedRows = 0, nextCursor = null)
        val emptyPlatformLogoResult = ServiceEtlTableSyncResult(
            processedRows = 0,
            nextCursor = null,
            note = "diff-based upsert: ingest.platform_logo has no updated_at cursor",
        )

        `when`(repository.syncGameStatuses()).thenReturn(emptyCursorResult)
        `when`(repository.syncGameTypes()).thenReturn(emptyCursorResult)
        `when`(repository.syncLanguages()).thenReturn(emptyCursorResult)
        `when`(repository.syncRegions()).thenReturn(emptyCursorResult)
        `when`(repository.syncReleaseRegions()).thenReturn(emptyCursorResult)
        `when`(repository.syncReleaseStatuses()).thenReturn(emptyCursorResult)
        `when`(repository.syncGenres()).thenReturn(emptyCursorResult)
        `when`(repository.syncThemes()).thenReturn(emptyCursorResult)
        `when`(repository.syncPlayerPerspectives()).thenReturn(emptyCursorResult)
        `when`(repository.syncGameModes()).thenReturn(emptyCursorResult)
        `when`(repository.syncKeywords()).thenReturn(emptyCursorResult)
        `when`(repository.syncLanguageSupportTypes()).thenReturn(emptyCursorResult)
        `when`(repository.syncWebsiteTypes()).thenReturn(emptyCursorResult)
        `when`(repository.syncPlatformLogos()).thenReturn(emptyPlatformLogoResult)
        `when`(repository.syncPlatformTypes()).thenReturn(emptyCursorResult)
        `when`(repository.syncPlatforms()).thenReturn(emptyCursorResult)
        `when`(repository.syncCompanies()).thenReturn(emptyCursorResult)
    }

    private fun emptySlice3CalculationResult(): AffectedGameIdCalculationResult =
        slice3CalculationResult(perTableGameIds = emptyMap())

    private fun slice3CalculationResult(
        perTableGameIds: Map<String, Set<Long>>,
        cursorFromByTable: Map<String, Long> = emptyMap(),
        cursorTo: Long = SLICE3_CURSOR_TO,
        noteByTable: Map<String, String> = emptyMap(),
    ): AffectedGameIdCalculationResult {
        val sourceResults = SLICE3_SOURCE_TABLES.map { tableName ->
            val materializedInCurrentSlice = tableName in SLICE4_CORE_SOURCE_TABLES
            AffectedGameIdSourceResult(
                tableName = tableName,
                cursorFrom = if (materializedInCurrentSlice) null else cursorFromByTable[tableName],
                cursorTo = if (materializedInCurrentSlice) null else cursorTo,
                affectedGameIds = perTableGameIds[tableName].orEmpty(),
                note = noteByTable[tableName]
                    ?: if (materializedInCurrentSlice) "slice4 projection diff test note" else "slice4 deferred dry-run test note",
                materializedInCurrentSlice = materializedInCurrentSlice,
                advanceCursor = false,
            )
        }
        val affectedGameIds = linkedSetOf<Long>()
        sourceResults
            .filter { it.materializedInCurrentSlice }
            .forEach { affectedGameIds += it.affectedGameIds }
        return AffectedGameIdCalculationResult(
            affectedGameIds = affectedGameIds,
            sourceResults = sourceResults,
        )
    }

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
}
