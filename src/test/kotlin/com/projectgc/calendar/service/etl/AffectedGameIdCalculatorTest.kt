package com.projectgc.calendar.service.etl

import com.projectgc.calendar.repository.etl.AlternativeNameProjectionRow
import com.projectgc.calendar.repository.etl.ArtworkProjectionRow
import com.projectgc.calendar.repository.etl.CoverProjectionRow
import com.projectgc.calendar.repository.etl.GameCompanyProjectionRow
import com.projectgc.calendar.repository.etl.GameLanguageProjectionRow
import com.projectgc.calendar.repository.etl.GameLocalizationProjectionRow
import com.projectgc.calendar.repository.etl.GameProjectionRow
import com.projectgc.calendar.repository.etl.GameReleaseProjectionRow
import com.projectgc.calendar.repository.etl.GameVideoProjectionRow
import com.projectgc.calendar.repository.etl.IngestEtlReadJdbcRepository
import com.projectgc.calendar.repository.etl.ScreenshotProjectionRow
import com.projectgc.calendar.repository.etl.ServiceEtlJdbcRepository
import com.projectgc.calendar.repository.etl.WebsiteProjectionRow
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
    private val serviceRepository = mock(ServiceEtlJdbcRepository::class.java)
    private val calculator = AffectedGameIdCalculator(ingestRepository, serviceRepository)

    @Test
    fun `returns slice6 materialized diffs for all source tables on initial execution`() {
        val allGameIds = listOf(11L, 22L, 33L)

        stubCommonInitialRows(allGameIds)
        `when`(serviceRepository.loadCurrentGameProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameReleaseProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameCompanyProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameLanguageProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameLocalizationProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameDimensionProjectionRows(anyObject(String::class.java), anyObject(String::class.java)))
            .thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameRelationProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentCoverProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentArtworkProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentScreenshotProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameVideoProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentWebsiteProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentAlternativeNameProjectionRows()).thenReturn(emptyList())

        val result = calculator.calculate(500L)
        val resultsByTable = result.sourceResults.associateBy { it.tableName }

        assertEquals(allGameIds.toSet(), result.affectedGameIds)
        assertEquals(SLICE6_SOURCE_TABLES, result.sourceResults.map { it.tableName })
        SLICE6_SOURCE_TABLES.forEach { tableName ->
            val sourceResult = resultsByTable.getValue(tableName)
            assertNull(sourceResult.cursorFrom)
            assertNull(sourceResult.cursorTo)
            assertEquals(allGameIds.toSet(), sourceResult.affectedGameIds)
            assertTrue(sourceResult.note.contains("slice6"))
            assertTrue(sourceResult.materializedInCurrentSlice)
            assertFalse(sourceResult.advanceCursor)
        }

        verify(ingestRepository).findAllIngestGameIds()
        verify(serviceRepository, never()).findCursor(anyObject(String::class.java))
        verify(ingestRepository, never()).findAffectedGameIdsFromGameUpdatedAt(anyLong())
    }

    @Test
    fun `includes media projection diffs in affected set when core projections are unchanged`() {
        val allGameIds = listOf(1L, 2L, 3L)

        `when`(ingestRepository.findAllIngestGameIds()).thenReturn(allGameIds)
        `when`(ingestRepository.loadAllGameProjectionRows()).thenReturn(allGameIds.map(::gameRow))
        `when`(serviceRepository.loadCurrentGameProjectionRows()).thenReturn(allGameIds.map(::gameRow))
        `when`(ingestRepository.loadAllGameReleaseProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameReleaseProjectionRows()).thenReturn(emptyList())
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

        `when`(ingestRepository.loadAllCoverProjectionRows()).thenReturn(
            listOf(CoverProjectionRow(id = 101L, gameId = 1L, gameLocalizationId = null, imageId = "cover-1", url = null, isMain = true))
        )
        `when`(serviceRepository.loadCurrentCoverProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllArtworkProjectionRows()).thenReturn(
            listOf(ArtworkProjectionRow(id = 201L, gameId = 2L, imageId = "art-2", url = null))
        )
        `when`(serviceRepository.loadCurrentArtworkProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllScreenshotProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentScreenshotProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllGameVideoProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameVideoProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllWebsiteProjectionRows()).thenReturn(
            listOf(WebsiteProjectionRow(id = 301L, gameId = 3L, typeId = 71L, url = "https://example.com", isTrusted = true))
        )
        `when`(serviceRepository.loadCurrentWebsiteProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllAlternativeNameProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentAlternativeNameProjectionRows()).thenReturn(emptyList())

        `when`(serviceRepository.loadIds(anyObject(String::class.java))).thenAnswer { invocation ->
            when (invocation.arguments[0] as String) {
                "service.website_type" -> setOf(71L)
                else -> emptySet<Long>()
            }
        }

        val result = calculator.calculate(700L)
        val resultsByTable = result.sourceResults.associateBy { it.tableName }

        assertEquals(linkedSetOf(1L, 2L, 3L), result.affectedGameIds)
        assertEquals(linkedSetOf(1L), resultsByTable.getValue("cover").affectedGameIds)
        assertEquals(linkedSetOf(2L), resultsByTable.getValue("artwork").affectedGameIds)
        assertEquals(linkedSetOf(3L), resultsByTable.getValue("website").affectedGameIds)
        assertTrue(resultsByTable.getValue("cover").note.contains("service.cover"))
        assertTrue(resultsByTable.getValue("website").note.contains("service.website"))
    }

    @Test
    fun `includes both old and new game ids when release row keeps the same key but changes game ownership`() {
        val allGameIds = listOf(1L, 2L)

        `when`(ingestRepository.findAllIngestGameIds()).thenReturn(allGameIds)
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
        `when`(ingestRepository.loadAllCoverProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentCoverProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllArtworkProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentArtworkProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllScreenshotProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentScreenshotProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllGameVideoProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentGameVideoProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllWebsiteProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentWebsiteProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllAlternativeNameProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadCurrentAlternativeNameProjectionRows()).thenReturn(emptyList())
        `when`(serviceRepository.loadIds(anyObject(String::class.java))).thenReturn(emptySet())

        val result = calculator.calculate(700L)
        val releaseSourceResult = result.sourceResults.first { it.tableName == "release_date" }

        assertEquals(linkedSetOf(2L, 1L), releaseSourceResult.affectedGameIds)
        assertEquals(linkedSetOf(2L, 1L), result.affectedGameIds)
    }

    private fun stubCommonInitialRows(allGameIds: List<Long>) {
        `when`(ingestRepository.findAllIngestGameIds()).thenReturn(allGameIds)
        `when`(ingestRepository.loadAllGameProjectionRows()).thenReturn(allGameIds.map(::gameRow))
        `when`(ingestRepository.loadAllGameReleaseProjectionRows()).thenReturn(allGameIds.map(::releaseRow))
        `when`(ingestRepository.loadAllGameCompanyProjectionRows()).thenReturn(allGameIds.map { companyRow(it, it + 90) })
        `when`(ingestRepository.loadAllGameLanguageProjectionRows()).thenReturn(allGameIds.map { languageRow(it, it + 30) })
        `when`(ingestRepository.loadAllGameLocalizationProjectionRows()).thenReturn(allGameIds.map(::localizationRow))
        `when`(ingestRepository.loadAllGameArrayProjectionRows(anyObject(String::class.java))).thenReturn(emptyList())
        `when`(ingestRepository.loadAllGameRelationProjectionRows()).thenReturn(emptyList())
        `when`(ingestRepository.loadAllCoverProjectionRows()).thenReturn(allGameIds.map(::coverRow))
        `when`(ingestRepository.loadAllArtworkProjectionRows()).thenReturn(allGameIds.map(::artworkRow))
        `when`(ingestRepository.loadAllScreenshotProjectionRows()).thenReturn(allGameIds.map(::screenshotRow))
        `when`(ingestRepository.loadAllGameVideoProjectionRows()).thenReturn(allGameIds.map(::gameVideoRow))
        `when`(ingestRepository.loadAllWebsiteProjectionRows()).thenReturn(allGameIds.map(::websiteRow))
        `when`(ingestRepository.loadAllAlternativeNameProjectionRows()).thenReturn(allGameIds.map(::alternativeNameRow))

        `when`(serviceRepository.loadIds(anyObject(String::class.java))).thenAnswer { invocation ->
            when (invocation.arguments[0] as String) {
                "service.company" -> allGameIds.mapTo(linkedSetOf()) { it + 90 }
                "service.language" -> allGameIds.mapTo(linkedSetOf()) { it + 30 }
                else -> emptySet<Long>()
            }
        }
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

    @Suppress("UNCHECKED_CAST")
    private fun <T> anyObject(type: Class<T>): T {
        org.mockito.Mockito.any(type)
        return null as T
    }
}
