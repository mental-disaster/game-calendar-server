package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import com.projectgc.calendar.repository.etl.ServiceEtlSourceLogEntry
import com.projectgc.calendar.repository.etl.ServiceEtlTableSyncResult
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doAnswer
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

class ServiceEtlServiceTest {
    private val repository = mock(ServiceEtlJdbcRepository::class.java)
    private val service = ServiceEtlService(
        serviceEtlJdbcRepository = repository,
        transactionTemplate = TransactionTemplate(NoOpTransactionManager()),
    )
    private val sourceLogs = mutableListOf<ServiceEtlSourceLogEntry>()
    private val cursorWrites = mutableListOf<Pair<String, Long>>()
    private val finishedStatuses = mutableListOf<String>()

    @BeforeEach
    fun setUp() {
        reset(repository)
        sourceLogs.clear()
        cursorWrites.clear()
        finishedStatuses.clear()
        `when`(repository.findCursor(anyObject(String::class.java))).thenReturn(null)
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
    fun `syncs slice2 sources, advances cursor-backed tables, and skips deferred tables`() {
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

        val runId = UUID.randomUUID()
        service.run(runId, ServiceEtlTrigger.manual())

        assertEquals(emptyList(), cursorWrites)
        verify(repository, never()).findCursor("game_status")
        verify(repository, never()).findCursor("keyword")
        verify(repository, never()).findCursor("platform_logo")
        verify(repository, never()).findCursor("company")

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

        val skippedGameLog = logsByTable.getValue("game")
        assertEquals("skipped", skippedGameLog.status)
        assertEquals(0, skippedGameLog.processedRows)
        assertNotNull(skippedGameLog.note)

        assertEquals(listOf("completed"), finishedStatuses)
    }

    @Test
    fun `keeps slice2 diff-based sources cursorless on rerun`() {
        `when`(repository.syncGameStatuses())
            .thenReturn(ServiceEtlTableSyncResult(processedRows = 0, nextCursor = null))

        val runId = UUID.randomUUID()
        service.run(runId, ServiceEtlTrigger.manual())

        assertEquals(28, sourceLogs.size)
        val gameStatusLog = sourceLogs.first { it.tableName == "game_status" }

        assertEquals("completed", gameStatusLog.status)
        assertEquals(0, gameStatusLog.processedRows)
        assertNull(gameStatusLog.cursorFrom)
        assertNull(gameStatusLog.cursorTo)

        assertEquals(emptyList(), cursorWrites)
        verify(repository, never()).findCursor("game_status")
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

    @Suppress("UNCHECKED_CAST")
    private fun <T> anyObject(type: Class<T>): T {
        org.mockito.Mockito.any(type)
        return null as T
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
