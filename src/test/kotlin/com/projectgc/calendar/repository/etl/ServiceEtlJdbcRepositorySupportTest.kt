package com.projectgc.calendar.repository.etl

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ServiceEtlJdbcRepositorySupportTest {

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
}
