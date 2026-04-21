package com.projectgc.calendar.repository.etl

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ServiceEtlJdbcRepositorySupportTest {

    @Test
    fun `resolveGameReferences nulls unresolved status and type foreign keys`() {
        val rows = listOf(
            GameProjectionRow(
                id = 1L,
                slug = "game-1",
                name = "Game 1",
                summary = null,
                storyline = null,
                firstReleaseDateEpochSecond = null,
                statusId = 10L,
                typeId = 20L,
                sourceUpdatedAtEpochSecond = 100L,
                tags = listOf(1L, 2L),
            ),
        )

        assertEquals(
            listOf(
                GameProjectionRow(
                    id = 1L,
                    slug = "game-1",
                    name = "Game 1",
                    summary = null,
                    storyline = null,
                    firstReleaseDateEpochSecond = null,
                    statusId = 10L,
                    typeId = null,
                    sourceUpdatedAtEpochSecond = 100L,
                    tags = listOf(1L, 2L),
                ),
            ),
            resolveGameReferences(
                rows = rows,
                availableStatusIds = setOf(10L),
                availableTypeIds = emptySet(),
            ),
        )
    }

    @Test
    fun `resolveCompanyReferences keeps self references only when target exists in ingest snapshot`() {
        val rows = listOf(
            CompanySyncRow(
                id = 10L,
                name = "child",
                parentCompanyId = 20L,
                mergedIntoCompanyId = 99L,
            ),
            CompanySyncRow(
                id = 20L,
                name = "parent",
                parentCompanyId = null,
                mergedIntoCompanyId = null,
            ),
        )

        assertEquals(
            listOf(
                CompanySyncRow(
                    id = 10L,
                    name = "child",
                    parentCompanyId = 20L,
                    mergedIntoCompanyId = null,
                ),
                CompanySyncRow(
                    id = 20L,
                    name = "parent",
                    parentCompanyId = null,
                    mergedIntoCompanyId = null,
                ),
            ),
            resolveCompanyReferences(rows),
        )
    }

    @Test
    fun `resolvePlatformReferences nulls unresolved foreign keys until dependency tables are synced`() {
        val rows = listOf(
            PlatformSyncRow(
                id = 1L,
                name = "platform",
                abbreviation = "P1",
                alternativeName = null,
                logoId = 11L,
                typeId = 12L,
            ),
        )

        assertEquals(
            listOf(
                PlatformSyncRow(
                    id = 1L,
                    name = "platform",
                    abbreviation = "P1",
                    alternativeName = null,
                    logoId = 11L,
                    typeId = null,
                ),
            ),
            resolvePlatformReferences(
                rows = rows,
                availableLogoIds = setOf(11L),
                availableTypeIds = emptySet(),
            ),
        )
    }

    @Test
    fun `resolveGameLocalizationReferences drops rows without parent game and nulls unresolved region`() {
        val rows = listOf(
            GameLocalizationProjectionRow(
                id = 11L,
                gameId = 1L,
                regionId = 21L,
                name = "Localized Name",
            ),
            GameLocalizationProjectionRow(
                id = 12L,
                gameId = 99L,
                regionId = 22L,
                name = "Orphan",
            ),
        )

        assertEquals(
            listOf(
                GameLocalizationProjectionRow(
                    id = 11L,
                    gameId = 1L,
                    regionId = null,
                    name = "Localized Name",
                ),
            ),
            resolveGameLocalizationReferences(
                rows = rows,
                availableGameIds = setOf(1L),
                availableRegionIds = emptySet(),
            ),
        )
    }

    @Test
    fun `resolveGameReleaseReferences drops rows without parent game and nulls unresolved dimensions`() {
        val rows = listOf(
            GameReleaseProjectionRow(
                id = 21L,
                gameId = 1L,
                platformId = 31L,
                regionId = 41L,
                statusId = 51L,
                releaseDateEpochSecond = 1_700_000_000L,
                year = 2024,
                month = 5,
                dateHuman = "2024-05",
            ),
            GameReleaseProjectionRow(
                id = 22L,
                gameId = 99L,
                platformId = 32L,
                regionId = 42L,
                statusId = 52L,
                releaseDateEpochSecond = null,
                year = null,
                month = null,
                dateHuman = null,
            ),
        )

        assertEquals(
            listOf(
                GameReleaseProjectionRow(
                    id = 21L,
                    gameId = 1L,
                    platformId = 31L,
                    regionId = null,
                    statusId = null,
                    releaseDateEpochSecond = 1_700_000_000L,
                    year = 2024,
                    month = 5,
                    dateHuman = "2024-05",
                ),
            ),
            resolveGameReleaseReferences(
                rows = rows,
                availableGameIds = setOf(1L),
                availablePlatformIds = setOf(31L),
                availableRegionIds = emptySet(),
                availableStatusIds = emptySet(),
            ),
        )
    }
}
