package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import org.junit.jupiter.api.Test
import org.mockito.Mockito.anyLong
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AffectedGameIdCalculatorTest {
    companion object {
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
    fun `returns all games for every slice3 source on initial execution`() {
        val allGameIds = listOf(11L, 22L, 33L)
        `when`(repository.findCursor(anyObject(String::class.java))).thenReturn(null)
        `when`(repository.findAllIngestGameIds()).thenReturn(allGameIds)

        val result = calculator.calculate(500L)

        assertEquals(allGameIds.toSet(), result.affectedGameIds)
        assertEquals(SLICE3_SOURCE_TABLES, result.sourceResults.map { it.tableName })
        result.sourceResults.forEach { sourceResult ->
            assertEquals(null, sourceResult.cursorFrom)
            assertEquals(500L, sourceResult.cursorTo)
            assertEquals(allGameIds.toSet(), sourceResult.affectedGameIds)
            assertTrue(sourceResult.note.contains("initial full sweep"))
        }

        verify(repository).findAllIngestGameIds()
        verify(repository, never()).findAffectedGameIdsFromGames(anyLong())
        verify(repository, never()).findAffectedGameIdsFromReleaseDates(anyLong())
        verify(repository, never()).findAffectedGameIdsFromInvolvedCompanies(anyLong())
        verify(repository, never()).findAffectedGameIdsFromLanguageSupports(anyLong())
        verify(repository, never()).findAffectedGameIdsFromGameLocalizations(anyLong())
        verify(repository, never()).findAffectedGameIdsFromGameUpdatedAt(anyLong())
    }

    @Test
    fun `mixes source deltas with per-source full sweep when a cursor is missing`() {
        val allGameIds = listOf(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
        `when`(repository.findAllIngestGameIds()).thenReturn(allGameIds)
        `when`(repository.findCursor("game")).thenReturn(10L)
        `when`(repository.findCursor("release_date")).thenReturn(20L)
        `when`(repository.findCursor("involved_company")).thenReturn(30L)
        `when`(repository.findCursor("language_support")).thenReturn(40L)
        `when`(repository.findCursor("game_localization")).thenReturn(50L)
        `when`(repository.findCursor("cover")).thenReturn(61L)
        `when`(repository.findCursor("artwork")).thenReturn(62L)
        `when`(repository.findCursor("screenshot")).thenReturn(63L)
        `when`(repository.findCursor("game_video")).thenReturn(64L)
        `when`(repository.findCursor("website")).thenReturn(null)
        `when`(repository.findCursor("alternative_name")).thenReturn(66L)
        `when`(repository.findAffectedGameIdsFromGames(10L)).thenReturn(linkedSetOf(1L, 2L))
        `when`(repository.findAffectedGameIdsFromReleaseDates(20L)).thenReturn(linkedSetOf(2L, 3L))
        `when`(repository.findAffectedGameIdsFromInvolvedCompanies(30L)).thenReturn(linkedSetOf(4L))
        `when`(repository.findAffectedGameIdsFromLanguageSupports(40L)).thenReturn(linkedSetOf(5L))
        `when`(repository.findAffectedGameIdsFromGameLocalizations(50L)).thenReturn(linkedSetOf(6L))
        `when`(repository.findAffectedGameIdsFromGameUpdatedAt(61L)).thenReturn(linkedSetOf(7L))
        `when`(repository.findAffectedGameIdsFromGameUpdatedAt(62L)).thenReturn(linkedSetOf(8L))
        `when`(repository.findAffectedGameIdsFromGameUpdatedAt(63L)).thenReturn(linkedSetOf(9L))
        `when`(repository.findAffectedGameIdsFromGameUpdatedAt(64L)).thenReturn(linkedSetOf(1L, 9L))
        `when`(repository.findAffectedGameIdsFromGameUpdatedAt(66L)).thenReturn(linkedSetOf(2L))

        val result = calculator.calculate(700L)
        val resultsByTable = result.sourceResults.associateBy { it.tableName }

        assertEquals(allGameIds.toSet(), result.affectedGameIds)
        assertEquals(linkedSetOf(1L, 2L), resultsByTable.getValue("game").affectedGameIds)
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
        assertEquals(null, resultsByTable.getValue("website").cursorFrom)
        assertEquals(700L, resultsByTable.getValue("website").cursorTo)
        assertTrue(resultsByTable.getValue("game").note.contains("source updated_at"))
        assertTrue(resultsByTable.getValue("cover").note.contains("ingest.game.updated_at"))
        assertTrue(resultsByTable.getValue("website").note.contains("initial full sweep"))

        verify(repository).findAllIngestGameIds()
        verify(repository).findAffectedGameIdsFromGames(10L)
        verify(repository).findAffectedGameIdsFromReleaseDates(20L)
        verify(repository).findAffectedGameIdsFromInvolvedCompanies(30L)
        verify(repository).findAffectedGameIdsFromLanguageSupports(40L)
        verify(repository).findAffectedGameIdsFromGameLocalizations(50L)
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
