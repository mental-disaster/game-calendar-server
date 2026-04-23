package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.GameCompanyProjectionRow
import com.projectgc.calendar.repository.etl.GameLanguageProjectionRow
import com.projectgc.calendar.repository.etl.GameLocalizationProjectionRow
import com.projectgc.calendar.repository.etl.GameProjectionRow
import com.projectgc.calendar.repository.etl.GameReleaseProjectionRow
import com.projectgc.calendar.repository.etl.IngestEtlReadJdbcRepository
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

    private val ingestRepository = mock(IngestEtlReadJdbcRepository::class.java)
    private val serviceRepository = mock(ServiceEtlJdbcRepository::class.java)
    private val calculator = AffectedGameIdCalculator(ingestRepository, serviceRepository)

    @Test
    fun `returns slice5 materialized diffs and keeps deferred slice6 sources in dry-run on initial execution`() {
        val allGameIds = listOf(11L, 22L, 33L)
        `when`(serviceRepository.findCursor(anyObject(String::class.java))).thenReturn(null)
        `when`(ingestRepository.findAllIngestGameIds()).thenReturn(allGameIds)
        `when`(ingestRepository.loadAllGameProjectionRows()).thenReturn(allGameIds.map(::gameRow))
        `when`(ingestRepository.loadAllGameReleaseProjectionRows()).thenReturn(allGameIds.map(::releaseRow))
        `when`(ingestRepository.loadAllGameCompanyProjectionRows()).thenReturn(
            listOf(
                companyRow(11L, 91L),
                companyRow(22L, 92L),
                companyRow(33L, 93L),
            )
        )
        `when`(ingestRepository.loadAllGameLanguageProjectionRows()).thenReturn(
            listOf(
                languageRow(11L, 31L),
                languageRow(22L, 32L),
                languageRow(33L, 33L),
            )
        )
        `when`(ingestRepository.loadAllGameLocalizationProjectionRows()).thenReturn(allGameIds.map(::localizationRow))
        `when`(ingestRepository.loadAllGameArrayProjectionRows(anyObject(String::class.java))).thenReturn(emptyList())
        `when`(ingestRepository.loadAllGameRelationProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameReleaseProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameCompanyProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameLanguageProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameLocalizationProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameDimensionProjectionRows(anyObject(String::class.java), anyObject(String::class.java)))
            .thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameRelationProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadIds(anyObject(String::class.java))).thenAnswer { invocation ->
            when (invocation.arguments[0] as String) {
                "service.company" -> setOf(91L, 92L, 93L)
                "service.language" -> setOf(31L, 32L, 33L)
                else -> emptySet<Long>()
            }
        }

        val result = calculator.calculate(500L)
        val resultsByTable = result.sourceResults.associateBy { it.tableName }

        assertEquals(allGameIds.toSet(), result.affectedGameIds)
        assertEquals(SLICE3_SOURCE_TABLES, result.sourceResults.map { it.tableName })
        assertEquals(allGameIds.toSet(), resultsByTable.getValue("game").affectedGameIds)
        assertEquals(allGameIds.toSet(), resultsByTable.getValue("release_date").affectedGameIds)
        assertEquals(allGameIds.toSet(), resultsByTable.getValue("involved_company").affectedGameIds)
        assertEquals(allGameIds.toSet(), resultsByTable.getValue("language_support").affectedGameIds)
        assertEquals(allGameIds.toSet(), resultsByTable.getValue("game_localization").affectedGameIds)

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

        verify(ingestRepository).findAllIngestGameIds()
        verify(serviceRepository, never()).findCursor("game")
        verify(serviceRepository, never()).findCursor("release_date")
        verify(serviceRepository, never()).findCursor("involved_company")
        verify(serviceRepository, never()).findCursor("language_support")
        verify(serviceRepository, never()).findCursor("game_localization")
        verify(ingestRepository, never()).findAffectedGameIdsFromGames(anyLong())
        verify(ingestRepository, never()).findAffectedGameIdsFromReleaseDates(anyLong())
        verify(ingestRepository, never()).findAffectedGameIdsFromInvolvedCompanies(anyLong())
        verify(ingestRepository, never()).findAffectedGameIdsFromLanguageSupports(anyLong())
        verify(ingestRepository, never()).findAffectedGameIdsFromGameLocalizations(anyLong())
        verify(ingestRepository, never()).findAffectedGameIdsFromGameUpdatedAt(anyLong())
    }

    @Test
    fun `mixes source deltas with per-source full sweep when a cursor is missing`() {
        val allGameIds = listOf(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
        `when`(ingestRepository.findAllIngestGameIds()).thenReturn(allGameIds)
        `when`(serviceRepository.findCursor("cover")).thenReturn(61L)
        `when`(serviceRepository.findCursor("artwork")).thenReturn(62L)
        `when`(serviceRepository.findCursor("screenshot")).thenReturn(63L)
        `when`(serviceRepository.findCursor("game_video")).thenReturn(64L)
        `when`(serviceRepository.findCursor("website")).thenReturn(null)
        `when`(serviceRepository.findCursor("alternative_name")).thenReturn(66L)

        `when`(ingestRepository.loadAllGameProjectionRows()).thenReturn(listOf(gameRow(1L), gameRow(2L), gameRow(3L)))
        `when`(ingestRepository.loadAllGameArrayProjectionRows(anyObject(String::class.java))).thenReturn(emptyList())
        `when`(ingestRepository.loadAllGameRelationProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameProjectionRows()).thenReturn(listOf(gameRow(1L), gameRow(2L)))

        `when`(ingestRepository.loadAllGameReleaseProjectionRows()).thenReturn(listOf(releaseRow(2L), releaseRow(3L)))
        `when`(serviceRepository.loadCurrentGameReleaseProjectionRows()).thenReturn(emptyList())

        `when`(ingestRepository.loadAllGameCompanyProjectionRows()).thenReturn(listOf(companyRow(4L, 91L)))
        `when`(serviceRepository.loadCurrentGameCompanyProjectionRows()).thenReturn(emptyList())

        `when`(ingestRepository.loadAllGameLanguageProjectionRows()).thenReturn(listOf(languageRow(5L, 31L)))
        `when`(serviceRepository.loadCurrentGameLanguageProjectionRows()).thenReturn(emptyList())

        `when`(ingestRepository.loadAllGameLocalizationProjectionRows()).thenReturn(listOf(localizationRow(6L)))
        `when`(serviceRepository.loadCurrentGameLocalizationProjectionRows()).thenReturn(emptyList())

        `when`(serviceRepository.loadCurrentGameDimensionProjectionRows(anyObject(String::class.java), anyObject(String::class.java)))
            .thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameRelationProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadIds(anyObject(String::class.java))).thenAnswer { invocation ->
            when (invocation.arguments[0] as String) {
                "service.company" -> setOf(91L)
                "service.language" -> setOf(31L)
                else -> emptySet<Long>()
            }
        }
        `when`(ingestRepository.findAffectedGameIdsFromGameUpdatedAt(61L)).thenReturn(linkedSetOf(7L))
        `when`(ingestRepository.findAffectedGameIdsFromGameUpdatedAt(62L)).thenReturn(linkedSetOf(8L))
        `when`(ingestRepository.findAffectedGameIdsFromGameUpdatedAt(63L)).thenReturn(linkedSetOf(9L))
        `when`(ingestRepository.findAffectedGameIdsFromGameUpdatedAt(64L)).thenReturn(linkedSetOf(1L, 9L))
        `when`(ingestRepository.findAffectedGameIdsFromGameUpdatedAt(66L)).thenReturn(linkedSetOf(2L))

        val result = calculator.calculate(700L)
        val resultsByTable = result.sourceResults.associateBy { it.tableName }

        assertEquals(linkedSetOf(3L, 2L, 4L, 5L, 6L), result.affectedGameIds)
        assertEquals(linkedSetOf(3L), resultsByTable.getValue("game").affectedGameIds)
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
        assertTrue(resultsByTable.getValue("game").note.contains("core and bridge projections"))
        assertTrue(resultsByTable.getValue("cover").note.contains("ingest.game.updated_at"))
        assertTrue(resultsByTable.getValue("website").note.contains("initial full sweep"))

        verify(ingestRepository).findAllIngestGameIds()
        verify(serviceRepository, never()).findCursor("game")
        verify(serviceRepository, never()).findCursor("release_date")
        verify(serviceRepository, never()).findCursor("involved_company")
        verify(serviceRepository, never()).findCursor("language_support")
        verify(serviceRepository, never()).findCursor("game_localization")
        verify(ingestRepository, never()).findAffectedGameIdsFromGames(anyLong())
        verify(ingestRepository, never()).findAffectedGameIdsFromReleaseDates(anyLong())
        verify(ingestRepository, never()).findAffectedGameIdsFromInvolvedCompanies(anyLong())
        verify(ingestRepository, never()).findAffectedGameIdsFromLanguageSupports(anyLong())
        verify(ingestRepository, never()).findAffectedGameIdsFromGameLocalizations(anyLong())
        verify(ingestRepository).findAffectedGameIdsFromGameUpdatedAt(61L)
        verify(ingestRepository).findAffectedGameIdsFromGameUpdatedAt(62L)
        verify(ingestRepository).findAffectedGameIdsFromGameUpdatedAt(63L)
        verify(ingestRepository).findAffectedGameIdsFromGameUpdatedAt(64L)
        verify(ingestRepository).findAffectedGameIdsFromGameUpdatedAt(66L)
    }

    @Test
    fun `includes both old and new game ids when release row keeps the same key but changes game ownership`() {
        val allGameIds = listOf(1L, 2L)
        `when`(ingestRepository.findAllIngestGameIds()).thenReturn(allGameIds)
        `when`(serviceRepository.findCursor(anyObject(String::class.java))).thenReturn(null)

        `when`(ingestRepository.loadAllGameProjectionRows()).thenReturn(allGameIds.map(::gameRow))
        `when`(serviceRepository.loadCurrentGameProjectionRows()).thenReturn(allGameIds.map(::gameRow))

        `when`(ingestRepository.loadAllGameReleaseProjectionRows()).thenReturn(
            listOf(
                GameReleaseProjectionRow(
                    id = 55L,
                    gameId = 2L,
                    platformId = null,
                    regionId = null,
                    statusId = null,
                    releaseDateEpochSecond = null,
                    year = null,
                    month = null,
                    dateHuman = null,
                )
            )
        )
        `when`(serviceRepository.loadCurrentGameReleaseProjectionRows()).thenReturn(
            listOf(
                GameReleaseProjectionRow(
                    id = 55L,
                    gameId = 1L,
                    platformId = null,
                    regionId = null,
                    statusId = null,
                    releaseDateEpochSecond = null,
                    year = null,
                    month = null,
                    dateHuman = null,
                )
            )
        )

        `when`(ingestRepository.loadAllGameCompanyProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameCompanyProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllGameLanguageProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameLanguageProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllGameLocalizationProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameLocalizationProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllGameArrayProjectionRows(anyObject(String::class.java))).thenReturn(emptyList())
        `when`(ingestRepository.loadAllGameRelationProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameDimensionProjectionRows(anyObject(String::class.java), anyObject(String::class.java)))
            .thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameRelationProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadIds(anyObject(String::class.java))).thenReturn(emptySet())

        val result = calculator.calculate(700L)
        val releaseSourceResult = result.sourceResults.first { it.tableName == "release_date" }

        assertEquals(linkedSetOf(2L, 1L), releaseSourceResult.affectedGameIds)
        assertEquals(linkedSetOf(2L, 1L), result.affectedGameIds)
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

    private fun companyRow(gameId: Long, companyId: Long) = GameCompanyProjectionRow(
        gameId = gameId,
        companyId = companyId,
        isDeveloper = true,
        isPublisher = false,
        isPorting = false,
        isSupporting = false,
    )

    private fun languageRow(gameId: Long, languageId: Long) = GameLanguageProjectionRow(
        gameId = gameId,
        languageId = languageId,
        supportsAudio = true,
        supportsSubtitles = false,
        supportsInterface = false,
    )

    private fun localizationRow(gameId: Long) = GameLocalizationProjectionRow(
        id = gameId,
        gameId = gameId,
        regionId = null,
        name = "loc-$gameId",
    )

    @Suppress("UNCHECKED_CAST")
    private fun <T> anyObject(type: Class<T>): T {
        org.mockito.Mockito.any(type)
        return null as T
    }
}
