package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import org.junit.jupiter.api.Test
import org.mockito.Mockito.anyLong
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class AffectedGameIdCalculatorTest {
    companion object {
        private val SLICE5_MATERIALIZED_SOURCE_TABLES = setOf(
            "game",
            "release_date",
            "involved_company",
            "language_support",
            "game_localization",
        )
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
    private val calculator = AffectedGameIdCalculator(repository)

    @Test
    fun `returns slice5 materialized diffs and keeps deferred slice6 sources in dry-run on initial execution`() {
        val allGameIds = listOf(11L, 22L, 33L)
        `when`(repository.findCursor(anyObject(String::class.java))).thenReturn(null)
        `when`(repository.findAllIngestGameIds()).thenReturn(allGameIds)
        `when`(repository.findAffectedGameIdsFromCoreGameProjectionDiff()).thenReturn(linkedSetOf(11L, 22L))
        `when`(repository.findAffectedGameIdsFromGameBridgeProjectionDiff()).thenReturn(linkedSetOf(22L, 33L))
        `when`(repository.findAffectedGameIdsFromGameReleaseProjectionDiff()).thenReturn(allGameIds.toSet())
        `when`(repository.findAffectedGameIdsFromInvolvedCompanyProjectionDiff()).thenReturn(allGameIds.toSet())
        `when`(repository.findAffectedGameIdsFromLanguageSupportProjectionDiff()).thenReturn(allGameIds.toSet())
        `when`(repository.findAffectedGameIdsFromGameLocalizationProjectionDiff()).thenReturn(allGameIds.toSet())

        val result = calculator.calculate(500L)
        val resultsByTable = result.sourceResults.associateBy { it.tableName }

        assertEquals(allGameIds.toSet(), result.affectedGameIds)
        assertEquals(SLICE3_SOURCE_TABLES, result.sourceResults.map { it.tableName })
        assertNull(resultsByTable.getValue("game").cursorFrom)
        assertNull(resultsByTable.getValue("game").cursorTo)
        assertEquals(allGameIds.toSet(), resultsByTable.getValue("game").affectedGameIds)
        assertTrue(resultsByTable.getValue("game").note.contains("core and bridge projections"))
        assertTrue(resultsByTable.getValue("game").materializedInCurrentSlice)
        assertFalse(resultsByTable.getValue("game").advanceCursor)

        assertNull(resultsByTable.getValue("release_date").cursorFrom)
        assertNull(resultsByTable.getValue("release_date").cursorTo)
        assertEquals(allGameIds.toSet(), resultsByTable.getValue("release_date").affectedGameIds)
        assertTrue(resultsByTable.getValue("release_date").note.contains("service.game_release"))
        assertTrue(resultsByTable.getValue("release_date").materializedInCurrentSlice)
        assertFalse(resultsByTable.getValue("release_date").advanceCursor)

        assertNull(resultsByTable.getValue("involved_company").cursorFrom)
        assertNull(resultsByTable.getValue("involved_company").cursorTo)
        assertEquals(allGameIds.toSet(), resultsByTable.getValue("involved_company").affectedGameIds)
        assertTrue(resultsByTable.getValue("involved_company").note.contains("service.game_company"))
        assertTrue(resultsByTable.getValue("involved_company").materializedInCurrentSlice)
        assertFalse(resultsByTable.getValue("involved_company").advanceCursor)

        assertNull(resultsByTable.getValue("language_support").cursorFrom)
        assertNull(resultsByTable.getValue("language_support").cursorTo)
        assertEquals(allGameIds.toSet(), resultsByTable.getValue("language_support").affectedGameIds)
        assertTrue(resultsByTable.getValue("language_support").note.contains("service.game_language"))
        assertTrue(resultsByTable.getValue("language_support").materializedInCurrentSlice)
        assertFalse(resultsByTable.getValue("language_support").advanceCursor)

        assertNull(resultsByTable.getValue("game_localization").cursorFrom)
        assertNull(resultsByTable.getValue("game_localization").cursorTo)
        assertEquals(allGameIds.toSet(), resultsByTable.getValue("game_localization").affectedGameIds)
        assertTrue(resultsByTable.getValue("game_localization").note.contains("service.game_localization"))
        assertTrue(resultsByTable.getValue("game_localization").materializedInCurrentSlice)
        assertFalse(resultsByTable.getValue("game_localization").advanceCursor)

        SLICE3_SOURCE_TABLES
            .filterNot { it in SLICE5_MATERIALIZED_SOURCE_TABLES }
            .forEach { tableName ->
                val sourceResult = resultsByTable.getValue(tableName)
                assertNull(sourceResult.cursorFrom)
                assertEquals(500L, sourceResult.cursorTo)
                assertEquals(allGameIds.toSet(), sourceResult.affectedGameIds)
                assertTrue(sourceResult.note.contains("dry-run"))
                assertFalse(sourceResult.materializedInCurrentSlice)
                assertFalse(sourceResult.advanceCursor)
            }

        verify(repository).findAllIngestGameIds()
        verify(repository).findAffectedGameIdsFromCoreGameProjectionDiff()
        verify(repository).findAffectedGameIdsFromGameBridgeProjectionDiff()
        verify(repository).findAffectedGameIdsFromGameReleaseProjectionDiff()
        verify(repository).findAffectedGameIdsFromInvolvedCompanyProjectionDiff()
        verify(repository).findAffectedGameIdsFromLanguageSupportProjectionDiff()
        verify(repository).findAffectedGameIdsFromGameLocalizationProjectionDiff()
        verify(repository, never()).findCursor("game")
        verify(repository, never()).findCursor("release_date")
        verify(repository, never()).findCursor("involved_company")
        verify(repository, never()).findCursor("language_support")
        verify(repository, never()).findCursor("game_localization")
        verify(repository, never()).findAffectedGameIdsFromGames(anyLong())
        verify(repository, never()).findAffectedGameIdsFromReleaseDates(anyLong())
        verify(repository, never()).findAffectedGameIdsFromInvolvedCompanies(anyLong())
        verify(repository, never()).findAffectedGameIdsFromLanguageSupports(anyLong())
        verify(repository, never()).findAffectedGameIdsFromGameUpdatedAt(anyLong())
    }

    @Test
    fun `mixes source deltas with per-source full sweep when a cursor is missing`() {
        val allGameIds = listOf(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
        `when`(repository.findAllIngestGameIds()).thenReturn(allGameIds)
        `when`(repository.findCursor("cover")).thenReturn(61L)
        `when`(repository.findCursor("artwork")).thenReturn(62L)
        `when`(repository.findCursor("screenshot")).thenReturn(63L)
        `when`(repository.findCursor("game_video")).thenReturn(64L)
        `when`(repository.findCursor("website")).thenReturn(null)
        `when`(repository.findCursor("alternative_name")).thenReturn(66L)
        `when`(repository.findAffectedGameIdsFromCoreGameProjectionDiff()).thenReturn(linkedSetOf(1L, 2L))
        `when`(repository.findAffectedGameIdsFromGameBridgeProjectionDiff()).thenReturn(linkedSetOf(3L))
        `when`(repository.findAffectedGameIdsFromGameReleaseProjectionDiff()).thenReturn(linkedSetOf(2L, 3L))
        `when`(repository.findAffectedGameIdsFromInvolvedCompanyProjectionDiff()).thenReturn(linkedSetOf(4L))
        `when`(repository.findAffectedGameIdsFromLanguageSupportProjectionDiff()).thenReturn(linkedSetOf(5L))
        `when`(repository.findAffectedGameIdsFromGameLocalizationProjectionDiff()).thenReturn(linkedSetOf(6L))
        `when`(repository.findAffectedGameIdsFromGameUpdatedAt(61L)).thenReturn(linkedSetOf(7L))
        `when`(repository.findAffectedGameIdsFromGameUpdatedAt(62L)).thenReturn(linkedSetOf(8L))
        `when`(repository.findAffectedGameIdsFromGameUpdatedAt(63L)).thenReturn(linkedSetOf(9L))
        `when`(repository.findAffectedGameIdsFromGameUpdatedAt(64L)).thenReturn(linkedSetOf(1L, 9L))
        `when`(repository.findAffectedGameIdsFromGameUpdatedAt(66L)).thenReturn(linkedSetOf(2L))

        val result = calculator.calculate(700L)
        val resultsByTable = result.sourceResults.associateBy { it.tableName }

        assertEquals(linkedSetOf(1L, 2L, 3L, 4L, 5L, 6L), result.affectedGameIds)
        assertEquals(linkedSetOf(1L, 2L, 3L), resultsByTable.getValue("game").affectedGameIds)
        assertEquals(linkedSetOf(2L, 3L), resultsByTable.getValue("release_date").affectedGameIds)
        assertEquals(linkedSetOf(4L), resultsByTable.getValue("involved_company").affectedGameIds)
        assertEquals(linkedSetOf(5L), resultsByTable.getValue("language_support").affectedGameIds)
        assertEquals(linkedSetOf(6L), resultsByTable.getValue("game_localization").affectedGameIds)
        assertEquals(linkedSetOf(7L), resultsByTable.getValue("cover").affectedGameIds)
        assertEquals(linkedSetOf(8L), resultsByTable.getValue("artwork").affectedGameIds)
        assertEquals(linkedSetOf(9L), resultsByTable.getValue("screenshot").affectedGameIds)
        assertEquals(linkedSetOf(1L, 9L), resultsByTable.getValue("game_video").affectedGameIds)
        assertEquals(allGameIds.toSet(), resultsByTable.getValue("website").affectedGameIds)
        assertEquals(linkedSetOf(2L), resultsByTable.getValue("alternative_name").affectedGameIds)
        assertNull(resultsByTable.getValue("game").cursorFrom)
        assertNull(resultsByTable.getValue("game").cursorTo)
        assertTrue(resultsByTable.getValue("game").materializedInCurrentSlice)
        assertFalse(resultsByTable.getValue("game").advanceCursor)
        assertNull(resultsByTable.getValue("release_date").cursorFrom)
        assertNull(resultsByTable.getValue("release_date").cursorTo)
        assertTrue(resultsByTable.getValue("release_date").materializedInCurrentSlice)
        assertFalse(resultsByTable.getValue("release_date").advanceCursor)
        assertNull(resultsByTable.getValue("involved_company").cursorFrom)
        assertNull(resultsByTable.getValue("involved_company").cursorTo)
        assertTrue(resultsByTable.getValue("involved_company").materializedInCurrentSlice)
        assertFalse(resultsByTable.getValue("involved_company").advanceCursor)
        assertNull(resultsByTable.getValue("language_support").cursorFrom)
        assertNull(resultsByTable.getValue("language_support").cursorTo)
        assertTrue(resultsByTable.getValue("language_support").materializedInCurrentSlice)
        assertFalse(resultsByTable.getValue("language_support").advanceCursor)
        assertEquals(null, resultsByTable.getValue("website").cursorFrom)
        assertEquals(700L, resultsByTable.getValue("website").cursorTo)
        assertFalse(resultsByTable.getValue("website").materializedInCurrentSlice)
        assertFalse(resultsByTable.getValue("website").advanceCursor)
        assertTrue(resultsByTable.getValue("game").note.contains("core and bridge projections"))
        assertTrue(resultsByTable.getValue("cover").note.contains("ingest.game.updated_at"))
        assertTrue(resultsByTable.getValue("website").note.contains("initial full sweep"))

        verify(repository).findAllIngestGameIds()
        verify(repository).findAffectedGameIdsFromCoreGameProjectionDiff()
        verify(repository).findAffectedGameIdsFromGameBridgeProjectionDiff()
        verify(repository).findAffectedGameIdsFromGameReleaseProjectionDiff()
        verify(repository).findAffectedGameIdsFromInvolvedCompanyProjectionDiff()
        verify(repository).findAffectedGameIdsFromLanguageSupportProjectionDiff()
        verify(repository).findAffectedGameIdsFromGameLocalizationProjectionDiff()
        verify(repository, never()).findCursor("game")
        verify(repository, never()).findCursor("release_date")
        verify(repository, never()).findCursor("involved_company")
        verify(repository, never()).findCursor("language_support")
        verify(repository, never()).findCursor("game_localization")
        verify(repository, never()).findAffectedGameIdsFromGames(anyLong())
        verify(repository, never()).findAffectedGameIdsFromReleaseDates(anyLong())
        verify(repository, never()).findAffectedGameIdsFromInvolvedCompanies(anyLong())
        verify(repository, never()).findAffectedGameIdsFromLanguageSupports(anyLong())
        verify(repository, never()).findAffectedGameIdsFromGameLocalizations(anyLong())
        verify(repository).findAffectedGameIdsFromGameUpdatedAt(61L)
        verify(repository).findAffectedGameIdsFromGameUpdatedAt(62L)
        verify(repository).findAffectedGameIdsFromGameUpdatedAt(63L)
        verify(repository).findAffectedGameIdsFromGameUpdatedAt(64L)
        verify(repository).findAffectedGameIdsFromGameUpdatedAt(66L)
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> anyObject(type: Class<T>): T {
        org.mockito.Mockito.any(type)
        return null as T
    }
}
