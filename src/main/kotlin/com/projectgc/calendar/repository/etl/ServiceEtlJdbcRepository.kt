package com.projectgc.calendar.repository.etl

import com.projectgc.calendar.service.etl.ServiceEtlTrigger
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ResultSetExtractor
import org.springframework.stereotype.Repository
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.sql.Types
import java.time.Instant
import java.util.UUID

@Repository
class ServiceEtlJdbcRepository(
    private val jdbc: JdbcTemplate,
) {
    companion object {
        private const val BATCH_SIZE = 50
        private const val DIFF_NOTE =
            "diff-based upsert: full ingest/service comparison avoids updated_at cursor gaps"
        private const val PLATFORM_LOGO_NOTE =
            "diff-based upsert: ingest.platform_logo has no updated_at cursor"
    }

    fun insertRunLog(runId: UUID, trigger: ServiceEtlTrigger, startedAt: Instant) {
        jdbc.update(
            """
            INSERT INTO service.etl_run_log
                (run_id, trigger_type, ingest_sync_id, started_at)
            VALUES (?, ?, ?, ?)
            """.trimIndent(),
            runId,
            trigger.type.name.lowercase(),
            trigger.ingestSyncId,
            Timestamp.from(startedAt),
        )
    }

    fun finishRunLog(
        runId: UUID,
        finishedAt: Instant,
        status: String,
        mismatchCount: Int = 0,
        errorMessage: String? = null,
    ) {
        jdbc.update(
            """
            UPDATE service.etl_run_log
            SET finished_at = ?, status = ?, mismatch_count = ?, error_message = ?
            WHERE run_id = ?
            """.trimIndent(),
            Timestamp.from(finishedAt),
            status,
            mismatchCount,
            errorMessage,
            runId,
        )
    }

    fun findCursor(tableName: String): Long? =
        jdbc.query(
            "SELECT last_synced_at FROM service.etl_cursor WHERE table_name = ?",
            ResultSetExtractor { rs -> if (rs.next()) rs.getLong("last_synced_at") else null },
            tableName,
        )

    fun upsertCursor(tableName: String, lastSyncedAt: Long, syncedAt: Instant) {
        jdbc.update(
            """
            INSERT INTO service.etl_cursor
                (table_name, last_synced_at, synced_at)
            VALUES (?, ?, ?)
            ON CONFLICT (table_name) DO UPDATE SET
                last_synced_at = EXCLUDED.last_synced_at,
                synced_at = EXCLUDED.synced_at
            """.trimIndent(),
            tableName,
            lastSyncedAt,
            Timestamp.from(syncedAt),
        )
    }

    fun insertSourceLog(entry: ServiceEtlSourceLogEntry) {
        jdbc.update(
            """
            INSERT INTO service.etl_source_log
                (run_id, table_name, status, processed_rows, cursor_from, cursor_to, note, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            entry.runId,
            entry.tableName,
            entry.status,
            entry.processedRows,
            entry.cursorFrom,
            entry.cursorTo,
            entry.note,
            Timestamp.from(entry.startedAt),
            Timestamp.from(entry.finishedAt),
        )
    }

    fun findAllIngestGameIds(): List<Long> =
        jdbc.query(
            """
            SELECT id
            FROM ingest.game
            ORDER BY id
            """.trimIndent(),
        ) { rs, _ -> rs.getLong("id") }

    fun findAffectedGameIdsFromGames(cursorFrom: Long): Set<Long> =
        jdbc.query(
            """
            SELECT id
            FROM ingest.game
            WHERE updated_at > ?
            ORDER BY id
            """.trimIndent(),
            { rs, _ -> rs.getLong("id") },
            cursorFrom,
        ).toCollection(linkedSetOf())

    fun findAffectedGameIdsFromReleaseDates(cursorFrom: Long): Set<Long> =
        findDistinctGameIdsByUpdatedAt("release_date", cursorFrom)

    fun findAffectedGameIdsFromInvolvedCompanies(cursorFrom: Long): Set<Long> =
        findDistinctGameIdsByUpdatedAt("involved_company", cursorFrom)

    fun findAffectedGameIdsFromLanguageSupports(cursorFrom: Long): Set<Long> =
        findDistinctGameIdsByUpdatedAt("language_support", cursorFrom)

    fun findAffectedGameIdsFromGameLocalizations(cursorFrom: Long): Set<Long> =
        findDistinctGameIdsByUpdatedAt("game_localization", cursorFrom)

    fun findAffectedGameIdsFromGameUpdatedAt(cursorFrom: Long): Set<Long> =
        findAffectedGameIdsFromGames(cursorFrom)

    fun syncGameStatuses() = syncNamedDimensionByDiff(
        sourceTable = "game_status",
        sourceValueColumn = "status",
        targetTable = "game_status",
        targetValueColumn = "status",
    )

    fun syncGameTypes() = syncNamedDimensionByDiff(
        sourceTable = "game_type",
        sourceValueColumn = "type",
        targetTable = "game_type",
        targetValueColumn = "type",
    )

    fun syncLanguages(): ServiceEtlTableSyncResult {
        val rows = jdbc.query(
            """
            SELECT i.id, i.locale, i.name, i.native_name
            FROM ingest.language i
            LEFT JOIN service.language s ON s.id = i.id
            WHERE s.id IS NULL
               OR ${differenceCondition("s.locale", "i.locale")}
               OR ${differenceCondition("s.name", "i.name")}
               OR ${differenceCondition("s.native_name", "i.native_name")}
            ORDER BY i.id
            """.trimIndent(),
        ) { rs, _ ->
            LanguageRow(
                id = rs.getLong("id"),
                locale = rs.getString("locale"),
                name = rs.getString("name"),
                nativeName = rs.getString("native_name"),
            )
        }
        batchUpsert(
            """
            INSERT INTO service.language (id, locale, name, native_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                locale = EXCLUDED.locale,
                name = EXCLUDED.name,
                native_name = EXCLUDED.native_name
            """.trimIndent(),
            rows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.locale)
            setNullableString(3, row.name)
            setNullableString(4, row.nativeName)
        }
        return diffResult(rows.size)
    }

    fun syncRegions(): ServiceEtlTableSyncResult {
        val rows = jdbc.query(
            """
            SELECT i.id, i.name, i.identifier
            FROM ingest.region i
            LEFT JOIN service.region s ON s.id = i.id
            WHERE s.id IS NULL
               OR ${differenceCondition("s.name", "i.name")}
               OR ${differenceCondition("s.identifier", "i.identifier")}
            ORDER BY i.id
            """.trimIndent(),
        ) { rs, _ ->
            RegionRow(
                id = rs.getLong("id"),
                name = rs.getString("name"),
                identifier = rs.getString("identifier"),
            )
        }
        batchUpsert(
            """
            INSERT INTO service.region (id, name, identifier)
            VALUES (?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                identifier = EXCLUDED.identifier
            """.trimIndent(),
            rows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.name)
            setNullableString(3, row.identifier)
        }
        return diffResult(rows.size)
    }

    fun syncReleaseRegions() = syncNamedDimensionByDiff(
        sourceTable = "release_date_region",
        sourceValueColumn = "region",
        targetTable = "release_region",
        targetValueColumn = "name",
    )

    fun syncReleaseStatuses(): ServiceEtlTableSyncResult {
        val rows = jdbc.query(
            """
            SELECT i.id, i.name, i.description
            FROM ingest.release_date_status i
            LEFT JOIN service.release_status s ON s.id = i.id
            WHERE s.id IS NULL
               OR ${differenceCondition("s.name", "i.name")}
               OR ${differenceCondition("s.description", "i.description")}
            ORDER BY i.id
            """.trimIndent(),
        ) { rs, _ ->
            ReleaseStatusRow(
                id = rs.getLong("id"),
                name = rs.getString("name"),
                description = rs.getString("description"),
            )
        }
        batchUpsert(
            """
            INSERT INTO service.release_status (id, name, description)
            VALUES (?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description
            """.trimIndent(),
            rows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.name)
            setNullableString(3, row.description)
        }
        return diffResult(rows.size)
    }

    fun syncGenres() = syncNamedDimensionByDiff(
        sourceTable = "genre",
        sourceValueColumn = "name",
        targetTable = "genre",
        targetValueColumn = "name",
    )

    fun syncThemes() = syncNamedDimensionByDiff(
        sourceTable = "theme",
        sourceValueColumn = "name",
        targetTable = "theme",
        targetValueColumn = "name",
    )

    fun syncPlayerPerspectives() = syncNamedDimensionByDiff(
        sourceTable = "player_perspective",
        sourceValueColumn = "name",
        targetTable = "player_perspective",
        targetValueColumn = "name",
    )

    fun syncGameModes() = syncNamedDimensionByDiff(
        sourceTable = "game_mode",
        sourceValueColumn = "name",
        targetTable = "game_mode",
        targetValueColumn = "name",
    )

    fun syncKeywords() = syncNamedDimensionByDiff(
        sourceTable = "keyword",
        sourceValueColumn = "name",
        targetTable = "keyword",
        targetValueColumn = "name",
    )

    fun syncLanguageSupportTypes() = syncNamedDimensionByDiff(
        sourceTable = "language_support_type",
        sourceValueColumn = "name",
        targetTable = "language_support_type",
        targetValueColumn = "name",
    )

    fun syncWebsiteTypes() = syncNamedDimensionByDiff(
        sourceTable = "website_type",
        sourceValueColumn = "type",
        targetTable = "website_type",
        targetValueColumn = "type",
    )

    fun syncPlatformLogos(): ServiceEtlTableSyncResult {
        val rows = jdbc.query(
            """
            SELECT i.id, i.image_id, i.url
            FROM ingest.platform_logo i
            LEFT JOIN service.platform_logo s ON s.id = i.id
            WHERE s.id IS NULL
               OR ${differenceCondition("s.image_id", "i.image_id")}
               OR ${differenceCondition("s.url", "i.url")}
            ORDER BY i.id
            """.trimIndent(),
        ) { rs, _ ->
            PlatformLogoRow(
                id = rs.getLong("id"),
                imageId = rs.getString("image_id"),
                url = rs.getString("url"),
            )
        }
        batchUpsert(
            """
            INSERT INTO service.platform_logo (id, image_id, url)
            VALUES (?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                image_id = EXCLUDED.image_id,
                url = EXCLUDED.url
            """.trimIndent(),
            rows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.imageId)
            setNullableString(3, row.url)
        }
        return diffResult(rows.size, note = PLATFORM_LOGO_NOTE)
    }

    fun syncPlatformTypes() = syncNamedDimensionByDiff(
        sourceTable = "platform_type",
        sourceValueColumn = "name",
        targetTable = "platform_type",
        targetValueColumn = "name",
    )

    fun syncPlatforms(): ServiceEtlTableSyncResult {
        val sourceRows = jdbc.query(
            """
            SELECT id, name, abbreviation, alternative_name, platform_logo, platform_type
            FROM ingest.platform
            ORDER BY id
            """.trimIndent(),
        ) { rs, _ ->
            PlatformSyncRow(
                id = rs.getLong("id"),
                name = rs.getString("name"),
                abbreviation = rs.getString("abbreviation"),
                alternativeName = rs.getString("alternative_name"),
                logoId = rs.getLong("platform_logo").takeIf { !rs.wasNull() },
                typeId = rs.getLong("platform_type").takeIf { !rs.wasNull() },
            )
        }
        val resolvedRows = resolvePlatformReferences(
            rows = sourceRows,
            availableLogoIds = loadIdSet("service.platform_logo"),
            availableTypeIds = loadIdSet("service.platform_type"),
        )
        val existingById = jdbc.query(
            """
            SELECT id, name, abbreviation, alternative_name, logo_id, type_id
            FROM service.platform
            """.trimIndent(),
        ) { rs, _ ->
            PlatformSyncRow(
                id = rs.getLong("id"),
                name = rs.getString("name"),
                abbreviation = rs.getString("abbreviation"),
                alternativeName = rs.getString("alternative_name"),
                logoId = rs.getLong("logo_id").takeIf { !rs.wasNull() },
                typeId = rs.getLong("type_id").takeIf { !rs.wasNull() },
            )
        }.associateBy { it.id }
        val changedRows = resolvedRows.filter { existingById[it.id] != it }

        batchUpsert(
            """
            INSERT INTO service.platform (id, name, abbreviation, alternative_name, logo_id, type_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                abbreviation = EXCLUDED.abbreviation,
                alternative_name = EXCLUDED.alternative_name,
                logo_id = EXCLUDED.logo_id,
                type_id = EXCLUDED.type_id
            """.trimIndent(),
            changedRows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.name)
            setNullableString(3, row.abbreviation)
            setNullableString(4, row.alternativeName)
            setNullableLong(5, row.logoId)
            setNullableLong(6, row.typeId)
        }
        return diffResult(changedRows.size)
    }

    fun syncCompanies(): ServiceEtlTableSyncResult {
        val sourceRows = jdbc.query(
            """
            SELECT id, name, parent, changed_company_id
            FROM ingest.company
            ORDER BY id
            """.trimIndent(),
        ) { rs, _ ->
            CompanySyncRow(
                id = rs.getLong("id"),
                name = rs.getString("name"),
                parentCompanyId = rs.getLong("parent").takeIf { !rs.wasNull() },
                mergedIntoCompanyId = rs.getLong("changed_company_id").takeIf { !rs.wasNull() },
            )
        }
        val resolvedRows = resolveCompanyReferences(sourceRows)
        val existingById = jdbc.query(
            """
            SELECT id, name, parent_company_id, merged_into_company_id
            FROM service.company
            """.trimIndent(),
        ) { rs, _ ->
            CompanySyncRow(
                id = rs.getLong("id"),
                name = rs.getString("name"),
                parentCompanyId = rs.getLong("parent_company_id").takeIf { !rs.wasNull() },
                mergedIntoCompanyId = rs.getLong("merged_into_company_id").takeIf { !rs.wasNull() },
            )
        }.associateBy { it.id }
        val changedRows = resolvedRows.filter { existingById[it.id] != it }

        batchUpsert(
            """
            INSERT INTO service.company (id, name, parent_company_id, merged_into_company_id)
            VALUES (?, ?, NULL, NULL)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name
            """.trimIndent(),
            changedRows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.name)
        }
        batchUpsert(
            """
            UPDATE service.company
            SET parent_company_id = ?, merged_into_company_id = ?
            WHERE id = ?
            """.trimIndent(),
            changedRows,
        ) { row ->
            setNullableLong(1, row.parentCompanyId)
            setNullableLong(2, row.mergedIntoCompanyId)
            setLong(3, row.id)
        }
        return diffResult(changedRows.size)
    }

    private fun syncNamedDimensionByDiff(
        sourceTable: String,
        sourceValueColumn: String,
        targetTable: String,
        targetValueColumn: String,
    ): ServiceEtlTableSyncResult {
        val rows = jdbc.query(
            """
            SELECT i.id, i.$sourceValueColumn AS value
            FROM ingest.$sourceTable i
            LEFT JOIN service.$targetTable s ON s.id = i.id
            WHERE s.id IS NULL
               OR ${differenceCondition("s.$targetValueColumn", "i.$sourceValueColumn")}
            ORDER BY i.id
            """.trimIndent(),
        ) { rs, _ ->
            NamedDimensionRow(
                id = rs.getLong("id"),
                value = rs.getString("value"),
            )
        }
        batchUpsert(
            """
            INSERT INTO service.$targetTable (id, $targetValueColumn)
            VALUES (?, ?)
            ON CONFLICT (id) DO UPDATE SET
                $targetValueColumn = EXCLUDED.$targetValueColumn
            """.trimIndent(),
            rows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.value)
        }
        return diffResult(rows.size)
    }

    private fun findDistinctGameIdsByUpdatedAt(tableName: String, cursorFrom: Long): Set<Long> =
        jdbc.query(
            """
            SELECT DISTINCT game
            FROM ingest.$tableName
            WHERE updated_at > ?
              AND game IS NOT NULL
            ORDER BY game
            """.trimIndent(),
            { rs, _ -> rs.getLong("game") },
            cursorFrom,
        ).toCollection(linkedSetOf())

    private fun loadIdSet(tableName: String): Set<Long> =
        jdbc.query("SELECT id FROM $tableName") { rs, _ -> rs.getLong("id") }.toSet()

    private fun differenceCondition(leftExpression: String, rightExpression: String): String =
        "NOT (($leftExpression = $rightExpression) OR ($leftExpression IS NULL AND $rightExpression IS NULL))"

    private fun diffResult(processedRows: Int, note: String = DIFF_NOTE) =
        ServiceEtlTableSyncResult(
            processedRows = processedRows,
            nextCursor = null,
            note = note,
        )

    private fun <T> batchUpsert(
        sql: String,
        rows: List<T>,
        setter: PreparedStatement.(T) -> Unit,
    ) {
        if (rows.isEmpty()) {
            return
        }
        jdbc.batchUpdate(sql, rows, BATCH_SIZE) { ps, row -> ps.setter(row) }
    }

    private fun PreparedStatement.setNullableLong(index: Int, value: Long?) {
        if (value != null) {
            setLong(index, value)
        } else {
            setNull(index, Types.BIGINT)
        }
    }

    private fun PreparedStatement.setNullableString(index: Int, value: String?) {
        if (value != null) {
            setString(index, value)
        } else {
            setNull(index, Types.VARCHAR)
        }
    }
}

data class ServiceEtlSourceLogEntry(
    val runId: UUID,
    val tableName: String,
    val status: String,
    val processedRows: Int,
    val cursorFrom: Long?,
    val cursorTo: Long?,
    val note: String?,
    val startedAt: Instant,
    val finishedAt: Instant,
)

data class ServiceEtlTableSyncResult(
    val processedRows: Int,
    val nextCursor: Long?,
    val note: String? = null,
)

private data class NamedDimensionRow(
    val id: Long,
    val value: String?,
)

private data class LanguageRow(
    val id: Long,
    val locale: String?,
    val name: String?,
    val nativeName: String?,
)

private data class RegionRow(
    val id: Long,
    val name: String?,
    val identifier: String?,
)

private data class ReleaseStatusRow(
    val id: Long,
    val name: String?,
    val description: String?,
)

private data class PlatformLogoRow(
    val id: Long,
    val imageId: String?,
    val url: String?,
)

internal data class PlatformSyncRow(
    val id: Long,
    val name: String?,
    val abbreviation: String?,
    val alternativeName: String?,
    val logoId: Long?,
    val typeId: Long?,
)

internal data class CompanySyncRow(
    val id: Long,
    val name: String?,
    val parentCompanyId: Long?,
    val mergedIntoCompanyId: Long?,
)

internal fun resolvePlatformReferences(
    rows: List<PlatformSyncRow>,
    availableLogoIds: Set<Long>,
    availableTypeIds: Set<Long>,
): List<PlatformSyncRow> = rows.map { row ->
    row.copy(
        logoId = row.logoId?.takeIf { it in availableLogoIds },
        typeId = row.typeId?.takeIf { it in availableTypeIds },
    )
}

internal fun resolveCompanyReferences(rows: List<CompanySyncRow>): List<CompanySyncRow> {
    val availableCompanyIds = rows.mapTo(mutableSetOf()) { it.id }
    return rows.map { row ->
        row.copy(
            parentCompanyId = row.parentCompanyId?.takeIf { it in availableCompanyIds },
            mergedIntoCompanyId = row.mergedIntoCompanyId?.takeIf { it in availableCompanyIds },
        )
    }
}
