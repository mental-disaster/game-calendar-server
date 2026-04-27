package com.projectgc.calendar.repository.etl

import com.projectgc.calendar.service.etl.ServiceEtlTrigger
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ResultSetExtractor
import org.springframework.stereotype.Repository
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Timestamp
import java.sql.Types
import java.time.Instant
import java.util.UUID

@Repository
class ServiceEtlJdbcRepository(
    @Qualifier("serviceJdbcTemplate")
    private val jdbc: JdbcTemplate,
) {
    companion object {
        private const val BATCH_SIZE = 50
        private const val PROJECTION_QUERY_CHUNK_SIZE = 500
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

    fun insertMismatchLogs(entries: List<ServiceEtlMismatchLogEntry>) {
        if (entries.isEmpty()) {
            return
        }
        jdbc.batchUpdate(
            """
            INSERT INTO service.etl_mismatch_log
                (run_id, table_name, mismatch_type, expected_value, actual_value, details, recorded_at)
            VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb), ?)
            """.trimIndent(),
            entries,
            BATCH_SIZE,
        ) { ps, entry ->
            ps.setObject(1, entry.runId)
            ps.setNullableString(2, entry.tableName)
            ps.setString(3, entry.mismatchType)
            ps.setNullableString(4, entry.expectedValue)
            ps.setNullableString(5, entry.actualValue)
            ps.setNullableString(6, entry.detailsJson)
            ps.setTimestamp(7, Timestamp.from(entry.recordedAt))
        }
    }

    fun syncGameStatuses(sourceRows: List<NamedDimensionRow>) = syncNamedDimensionByDiff(
        sourceRows = sourceRows,
        targetTable = "game_status",
        targetValueColumn = "status",
    )

    fun syncGameTypes(sourceRows: List<NamedDimensionRow>) = syncNamedDimensionByDiff(
        sourceRows = sourceRows,
        targetTable = "game_type",
        targetValueColumn = "type",
    )

    fun syncLanguages(sourceRows: List<LanguageRow>): ServiceEtlTableSyncResult {
        val existingById = loadCurrentLanguagesById()
        val changedRows = sourceRows.filter { existingById[it.id] != it }
        batchUpsert(
            """
            INSERT INTO service.language (id, locale, name, native_name)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                locale = EXCLUDED.locale,
                name = EXCLUDED.name,
                native_name = EXCLUDED.native_name
            """.trimIndent(),
            changedRows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.locale)
            setNullableString(3, row.name)
            setNullableString(4, row.nativeName)
        }
        return diffResult(changedRows.size)
    }

    fun syncRegions(sourceRows: List<RegionRow>): ServiceEtlTableSyncResult {
        val existingById = loadCurrentRegionsById()
        val changedRows = sourceRows.filter { existingById[it.id] != it }
        batchUpsert(
            """
            INSERT INTO service.region (id, name, identifier)
            VALUES (?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                identifier = EXCLUDED.identifier
            """.trimIndent(),
            changedRows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.name)
            setNullableString(3, row.identifier)
        }
        return diffResult(changedRows.size)
    }

    fun syncReleaseRegions(sourceRows: List<NamedDimensionRow>) = syncNamedDimensionByDiff(
        sourceRows = sourceRows,
        targetTable = "release_region",
        targetValueColumn = "name",
    )

    fun syncReleaseStatuses(sourceRows: List<ReleaseStatusRow>): ServiceEtlTableSyncResult {
        val existingById = loadCurrentReleaseStatusesById()
        val changedRows = sourceRows.filter { existingById[it.id] != it }
        batchUpsert(
            """
            INSERT INTO service.release_status (id, name, description)
            VALUES (?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description
            """.trimIndent(),
            changedRows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.name)
            setNullableString(3, row.description)
        }
        return diffResult(changedRows.size)
    }

    fun syncGenres(sourceRows: List<NamedDimensionRow>) = syncNamedDimensionByDiff(
        sourceRows = sourceRows,
        targetTable = "genre",
        targetValueColumn = "name",
    )

    fun syncThemes(sourceRows: List<NamedDimensionRow>) = syncNamedDimensionByDiff(
        sourceRows = sourceRows,
        targetTable = "theme",
        targetValueColumn = "name",
    )

    fun syncPlayerPerspectives(sourceRows: List<NamedDimensionRow>) = syncNamedDimensionByDiff(
        sourceRows = sourceRows,
        targetTable = "player_perspective",
        targetValueColumn = "name",
    )

    fun syncGameModes(sourceRows: List<NamedDimensionRow>) = syncNamedDimensionByDiff(
        sourceRows = sourceRows,
        targetTable = "game_mode",
        targetValueColumn = "name",
    )

    fun syncKeywords(sourceRows: List<NamedDimensionRow>) = syncNamedDimensionByDiff(
        sourceRows = sourceRows,
        targetTable = "keyword",
        targetValueColumn = "name",
    )

    fun syncLanguageSupportTypes(sourceRows: List<NamedDimensionRow>) = syncNamedDimensionByDiff(
        sourceRows = sourceRows,
        targetTable = "language_support_type",
        targetValueColumn = "name",
    )

    fun syncWebsiteTypes(sourceRows: List<NamedDimensionRow>) = syncNamedDimensionByDiff(
        sourceRows = sourceRows,
        targetTable = "website_type",
        targetValueColumn = "type",
    )

    fun syncPlatformLogos(sourceRows: List<PlatformLogoRow>): ServiceEtlTableSyncResult {
        val existingById = loadCurrentPlatformLogosById()
        val changedRows = sourceRows.filter { existingById[it.id] != it }
        batchUpsert(
            """
            INSERT INTO service.platform_logo (id, image_id, url)
            VALUES (?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                image_id = EXCLUDED.image_id,
                url = EXCLUDED.url
            """.trimIndent(),
            changedRows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.imageId)
            setNullableString(3, row.url)
        }
        return diffResult(changedRows.size, note = PLATFORM_LOGO_NOTE)
    }

    fun syncPlatformTypes(sourceRows: List<NamedDimensionRow>) = syncNamedDimensionByDiff(
        sourceRows = sourceRows,
        targetTable = "platform_type",
        targetValueColumn = "name",
    )

    fun syncPlatforms(
        sourceRows: List<PlatformSyncRow>,
        availableLogoIds: Set<Long> = loadIds("service.platform_logo"),
        availableTypeIds: Set<Long> = loadIds("service.platform_type"),
    ): ServiceEtlTableSyncResult {
        val resolvedRows = resolvePlatformReferences(
            rows = sourceRows,
            availableLogoIds = availableLogoIds,
            availableTypeIds = availableTypeIds,
        )
        val existingById = loadCurrentPlatformsById()
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

    fun syncCompanies(sourceRows: List<CompanySyncRow>): ServiceEtlTableSyncResult {
        val resolvedRows = resolveCompanyReferences(sourceRows)
        val existingById = loadCurrentCompaniesById()
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

    fun loadIds(tableName: String): Set<Long> =
        jdbc.query("SELECT id FROM $tableName") { rs, _ -> rs.getLong("id") }.toSet()

    fun deleteIds(tableName: String, ids: Set<Long>) {
        if (ids.isEmpty()) {
            return
        }
        ids.toList().chunked(PROJECTION_QUERY_CHUNK_SIZE).forEach { chunk ->
            val placeholders = chunk.joinToString(",") { "?" }
            jdbc.update(
                "DELETE FROM $tableName WHERE id IN ($placeholders)",
                *chunk.toTypedArray(),
            )
        }
    }

    fun loadCurrentNamedDimensionRows(
        tableName: String,
        targetValueColumn: String,
    ): List<NamedDimensionRow> =
        jdbc.query(
            """
            SELECT id, $targetValueColumn AS value
            FROM service.$tableName
            ORDER BY id
            """.trimIndent(),
        ) { rs, _ ->
            NamedDimensionRow(
                id = rs.getLong("id"),
                value = rs.getString("value"),
            )
        }

    fun loadCurrentLanguages(): List<LanguageRow> =
        loadCurrentLanguagesById()
            .values
            .sortedBy { it.id }

    fun loadCurrentRegions(): List<RegionRow> =
        loadCurrentRegionsById()
            .values
            .sortedBy { it.id }

    fun loadCurrentReleaseStatuses(): List<ReleaseStatusRow> =
        loadCurrentReleaseStatusesById()
            .values
            .sortedBy { it.id }

    fun loadCurrentPlatformLogos(): List<PlatformLogoRow> =
        loadCurrentPlatformLogosById()
            .values
            .sortedBy { it.id }

    fun loadCurrentPlatforms(): List<PlatformSyncRow> =
        loadCurrentPlatformsById()
            .values
            .sortedBy { it.id }

    fun loadCurrentCompanies(): List<CompanySyncRow> =
        loadCurrentCompaniesById()
            .values
            .sortedBy { it.id }

    fun loadCurrentGameProjectionRows(): List<GameProjectionRow> =
        jdbc.query(
            """
            SELECT
                id,
                slug,
                name,
                summary,
                storyline,
                EXTRACT(EPOCH FROM first_release_date)::BIGINT AS first_release_date,
                status_id,
                type_id,
                EXTRACT(EPOCH FROM source_updated_at)::BIGINT AS source_updated_at,
                tags
            FROM service.game
            ORDER BY id
            """.trimIndent(),
        ) { rs, _ ->
            GameProjectionRow(
                id = rs.getLong("id"),
                slug = rs.getString("slug"),
                name = rs.getString("name"),
                summary = rs.getString("summary"),
                storyline = rs.getString("storyline"),
                firstReleaseDateEpochSecond = rs.getLong("first_release_date").takeIf { !rs.wasNull() },
                statusId = rs.getLong("status_id").takeIf { !rs.wasNull() },
                typeId = rs.getLong("type_id").takeIf { !rs.wasNull() },
                sourceUpdatedAtEpochSecond = rs.getLong("source_updated_at").takeIf { !rs.wasNull() },
                tags = rs.getNullableLongList("tags"),
            )
        }

    fun loadCurrentGameReleaseProjectionRows(): List<GameReleaseProjectionRow> =
        jdbc.query(
            """
            SELECT
                id,
                game_id,
                platform_id,
                region_id,
                status_id,
                EXTRACT(EPOCH FROM release_date)::BIGINT AS release_date,
                year,
                month,
                date_human
            FROM service.game_release
            ORDER BY game_id, id
            """.trimIndent(),
        ) { rs, _ ->
            GameReleaseProjectionRow(
                id = rs.getLong("id"),
                gameId = rs.getLong("game_id"),
                platformId = rs.getLong("platform_id").takeIf { !rs.wasNull() },
                regionId = rs.getLong("region_id").takeIf { !rs.wasNull() },
                statusId = rs.getLong("status_id").takeIf { !rs.wasNull() },
                releaseDateEpochSecond = rs.getLong("release_date").takeIf { !rs.wasNull() },
                year = rs.getInt("year").takeIf { !rs.wasNull() },
                month = rs.getInt("month").takeIf { !rs.wasNull() },
                dateHuman = rs.getString("date_human"),
            )
        }

    fun loadCurrentGameLocalizationProjectionRows(): List<GameLocalizationProjectionRow> =
        jdbc.query(
            """
            SELECT id, game_id, region_id, name
            FROM service.game_localization
            ORDER BY game_id, id
            """.trimIndent(),
        ) { rs, _ ->
            GameLocalizationProjectionRow(
                id = rs.getLong("id"),
                gameId = rs.getLong("game_id"),
                regionId = rs.getLong("region_id").takeIf { !rs.wasNull() },
                name = rs.getString("name"),
            )
        }

    fun loadCurrentGameLanguageProjectionRows(): List<GameLanguageProjectionRow> =
        jdbc.query(
            """
            SELECT game_id, language_id, supports_audio, supports_subtitles, supports_interface
            FROM service.game_language
            ORDER BY game_id, language_id
            """.trimIndent(),
        ) { rs, _ ->
            GameLanguageProjectionRow(
                gameId = rs.getLong("game_id"),
                languageId = rs.getLong("language_id"),
                supportsAudio = rs.getBoolean("supports_audio"),
                supportsSubtitles = rs.getBoolean("supports_subtitles"),
                supportsInterface = rs.getBoolean("supports_interface"),
            )
        }

    fun loadCurrentGameDimensionProjectionRows(
        tableName: String,
        targetColumn: String,
    ): List<GameDimensionProjectionRow> =
        jdbc.query(
            """
            SELECT game_id, $targetColumn AS dimension_id
            FROM service.$tableName
            ORDER BY game_id, dimension_id
            """.trimIndent(),
        ) { rs, _ ->
            GameDimensionProjectionRow(
                gameId = rs.getLong("game_id"),
                dimensionId = rs.getLong("dimension_id"),
            )
        }

    fun loadCurrentGameCompanyProjectionRows(): List<GameCompanyProjectionRow> =
        jdbc.query(
            """
            SELECT game_id, company_id, is_developer, is_publisher, is_porting, is_supporting
            FROM service.game_company
            ORDER BY game_id, company_id
            """.trimIndent(),
        ) { rs, _ ->
            GameCompanyProjectionRow(
                gameId = rs.getLong("game_id"),
                companyId = rs.getLong("company_id"),
                isDeveloper = rs.getBoolean("is_developer"),
                isPublisher = rs.getBoolean("is_publisher"),
                isPorting = rs.getBoolean("is_porting"),
                isSupporting = rs.getBoolean("is_supporting"),
            )
        }

    fun loadCurrentGameRelationProjectionRows(): List<GameRelationProjectionRow> =
        jdbc.query(
            """
            SELECT game_id, related_game_id, relation_type
            FROM service.game_relation
            ORDER BY game_id, relation_type, related_game_id
            """.trimIndent(),
        ) { rs, _ ->
            GameRelationProjectionRow(
                gameId = rs.getLong("game_id"),
                relatedGameId = rs.getLong("related_game_id"),
                relationType = rs.getString("relation_type"),
            )
        }

    fun loadCurrentCoverProjectionRows(): List<CoverProjectionRow> =
        jdbc.query(
            """
            SELECT id, game_id, game_localization_id, image_id, url, is_main
            FROM service.cover
            ORDER BY game_id, id
            """.trimIndent(),
        ) { rs, _ ->
            CoverProjectionRow(
                id = rs.getLong("id"),
                gameId = rs.getLong("game_id"),
                gameLocalizationId = rs.getLong("game_localization_id").takeIf { !rs.wasNull() },
                imageId = rs.getString("image_id"),
                url = rs.getString("url"),
                isMain = rs.getBoolean("is_main"),
            )
        }

    fun loadCurrentArtworkProjectionRows(): List<ArtworkProjectionRow> =
        jdbc.query(
            """
            SELECT id, game_id, image_id, url
            FROM service.artwork
            ORDER BY game_id, id
            """.trimIndent(),
        ) { rs, _ ->
            ArtworkProjectionRow(
                id = rs.getLong("id"),
                gameId = rs.getLong("game_id"),
                imageId = rs.getString("image_id"),
                url = rs.getString("url"),
            )
        }

    fun loadCurrentScreenshotProjectionRows(): List<ScreenshotProjectionRow> =
        jdbc.query(
            """
            SELECT id, game_id, image_id, url
            FROM service.screenshot
            ORDER BY game_id, id
            """.trimIndent(),
        ) { rs, _ ->
            ScreenshotProjectionRow(
                id = rs.getLong("id"),
                gameId = rs.getLong("game_id"),
                imageId = rs.getString("image_id"),
                url = rs.getString("url"),
            )
        }

    fun loadCurrentGameVideoProjectionRows(): List<GameVideoProjectionRow> =
        jdbc.query(
            """
            SELECT id, game_id, name, video_id
            FROM service.game_video
            ORDER BY game_id, id
            """.trimIndent(),
        ) { rs, _ ->
            GameVideoProjectionRow(
                id = rs.getLong("id"),
                gameId = rs.getLong("game_id"),
                name = rs.getString("name"),
                videoId = rs.getString("video_id"),
            )
        }

    fun loadCurrentWebsiteProjectionRows(): List<WebsiteProjectionRow> =
        jdbc.query(
            """
            SELECT id, game_id, type_id, url, is_trusted
            FROM service.website
            ORDER BY game_id, id
            """.trimIndent(),
        ) { rs, _ ->
            WebsiteProjectionRow(
                id = rs.getLong("id"),
                gameId = rs.getLong("game_id"),
                typeId = rs.getLong("type_id").takeIf { !rs.wasNull() },
                url = rs.getString("url"),
                isTrusted = rs.getBoolean("is_trusted"),
            )
        }

    fun loadCurrentAlternativeNameProjectionRows(): List<AlternativeNameProjectionRow> =
        jdbc.query(
            """
            SELECT id, game_id, name, comment
            FROM service.alternative_name
            ORDER BY game_id, id
            """.trimIndent(),
        ) { rs, _ ->
            AlternativeNameProjectionRow(
                id = rs.getLong("id"),
                gameId = rs.getLong("game_id"),
                name = rs.getString("name"),
                comment = rs.getString("comment"),
            )
        }

    fun rebuildCoreGameProjections(
        gameRows: List<GameProjectionRow>,
        gameLocalizationRows: List<GameLocalizationProjectionRow>,
        gameReleaseRows: List<GameReleaseProjectionRow>,
        availableStatusIds: Set<Long> = loadIds("service.game_status"),
        availableTypeIds: Set<Long> = loadIds("service.game_type"),
        availableRegionIds: Set<Long> = loadIds("service.region"),
        availablePlatformIds: Set<Long> = loadIds("service.platform"),
        availableReleaseRegionIds: Set<Long> = loadIds("service.release_region"),
        availableReleaseStatusIds: Set<Long> = loadIds("service.release_status"),
    ) {
        val materializedGameIds = gameRows.mapTo(linkedSetOf()) { it.id }
        if (materializedGameIds.isEmpty()) {
            return
        }

        val resolvedGameRows = resolveGameReferences(
            rows = gameRows,
            availableStatusIds = availableStatusIds,
            availableTypeIds = availableTypeIds,
        )
        batchUpsert(
            """
            INSERT INTO service.game
                (id, slug, name, summary, storyline, first_release_date, status_id, type_id, source_updated_at, tags)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                summary = EXCLUDED.summary,
                storyline = EXCLUDED.storyline,
                first_release_date = EXCLUDED.first_release_date,
                status_id = EXCLUDED.status_id,
                type_id = EXCLUDED.type_id,
                source_updated_at = EXCLUDED.source_updated_at,
                tags = EXCLUDED.tags,
                updated_at = now()
            """.trimIndent(),
            resolvedGameRows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.slug)
            setNullableString(3, row.name)
            setNullableString(4, row.summary)
            setNullableString(5, row.storyline)
            setNullableInstantFromEpochSecond(6, row.firstReleaseDateEpochSecond)
            setNullableLong(7, row.statusId)
            setNullableLong(8, row.typeId)
            setNullableInstantFromEpochSecond(9, row.sourceUpdatedAtEpochSecond)
            setNullableLongArray(10, row.tags)
        }

        deleteByGameIds("service.game_localization", materializedGameIds)
        val resolvedGameLocalizationRows = resolveGameLocalizationReferences(
            rows = gameLocalizationRows,
            availableGameIds = materializedGameIds,
            availableRegionIds = availableRegionIds,
        )
        batchUpsert(
            """
            INSERT INTO service.game_localization (id, game_id, region_id, name)
            VALUES (?, ?, ?, ?)
            """.trimIndent(),
            resolvedGameLocalizationRows,
        ) { row ->
            setLong(1, row.id)
            setLong(2, row.gameId)
            setNullableLong(3, row.regionId)
            setNullableString(4, row.name)
        }

        deleteByGameIds("service.game_release", materializedGameIds)
        val resolvedGameReleaseRows = resolveGameReleaseReferences(
            rows = gameReleaseRows,
            availableGameIds = materializedGameIds,
            availablePlatformIds = availablePlatformIds,
            availableRegionIds = availableReleaseRegionIds,
            availableStatusIds = availableReleaseStatusIds,
        )
        batchUpsert(
            """
            INSERT INTO service.game_release
                (id, game_id, platform_id, region_id, status_id, release_date, year, month, date_human)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            resolvedGameReleaseRows,
        ) { row ->
            setLong(1, row.id)
            setLong(2, row.gameId)
            setNullableLong(3, row.platformId)
            setNullableLong(4, row.regionId)
            setNullableLong(5, row.statusId)
            setNullableInstantFromEpochSecond(6, row.releaseDateEpochSecond)
            setNullableInt(7, row.year)
            setNullableInt(8, row.month)
            setNullableString(9, row.dateHuman)
        }
    }

    fun rebuildGameDependentBridgeProjections(
        materializedGameIds: Set<Long>,
        gameLanguageRows: List<GameLanguageProjectionRow>,
        gameGenreRows: List<GameDimensionProjectionRow>,
        gameThemeRows: List<GameDimensionProjectionRow>,
        gamePlayerPerspectiveRows: List<GameDimensionProjectionRow>,
        gameModeRows: List<GameDimensionProjectionRow>,
        gameKeywordRows: List<GameDimensionProjectionRow>,
        gameCompanyRows: List<GameCompanyProjectionRow>,
        gameRelationRows: List<GameRelationProjectionRow>,
        availableGameIds: Set<Long> = loadIds("service.game"),
        availableLanguageIds: Set<Long> = loadIds("service.language"),
        availableGenreIds: Set<Long> = loadIds("service.genre"),
        availableThemeIds: Set<Long> = loadIds("service.theme"),
        availablePlayerPerspectiveIds: Set<Long> = loadIds("service.player_perspective"),
        availableGameModeIds: Set<Long> = loadIds("service.game_mode"),
        availableKeywordIds: Set<Long> = loadIds("service.keyword"),
        availableCompanyIds: Set<Long> = loadIds("service.company"),
    ) {
        if (materializedGameIds.isEmpty()) {
            return
        }

        deleteByGameIds("service.game_language", materializedGameIds)
        val resolvedGameLanguageRows = resolveGameLanguageReferences(
            rows = gameLanguageRows,
            availableGameIds = availableGameIds,
            availableLanguageIds = availableLanguageIds,
        )
        batchUpsert(
            """
            INSERT INTO service.game_language
                (game_id, language_id, supports_audio, supports_subtitles, supports_interface)
            VALUES (?, ?, ?, ?, ?)
            """.trimIndent(),
            resolvedGameLanguageRows,
        ) { row ->
            setLong(1, row.gameId)
            setLong(2, row.languageId)
            setBoolean(3, row.supportsAudio)
            setBoolean(4, row.supportsSubtitles)
            setBoolean(5, row.supportsInterface)
        }

        rebuildGameArrayBridgeProjection(
            tableName = "service.game_genre",
            targetColumn = "genre_id",
            materializedGameIds = materializedGameIds,
            sourceRows = gameGenreRows,
            availableGameIds = availableGameIds,
            availableDimensionIds = availableGenreIds,
        )
        rebuildGameArrayBridgeProjection(
            tableName = "service.game_theme",
            targetColumn = "theme_id",
            materializedGameIds = materializedGameIds,
            sourceRows = gameThemeRows,
            availableGameIds = availableGameIds,
            availableDimensionIds = availableThemeIds,
        )
        rebuildGameArrayBridgeProjection(
            tableName = "service.game_player_perspective",
            targetColumn = "player_perspective_id",
            materializedGameIds = materializedGameIds,
            sourceRows = gamePlayerPerspectiveRows,
            availableGameIds = availableGameIds,
            availableDimensionIds = availablePlayerPerspectiveIds,
        )
        rebuildGameArrayBridgeProjection(
            tableName = "service.game_game_mode",
            targetColumn = "game_mode_id",
            materializedGameIds = materializedGameIds,
            sourceRows = gameModeRows,
            availableGameIds = availableGameIds,
            availableDimensionIds = availableGameModeIds,
        )
        rebuildGameArrayBridgeProjection(
            tableName = "service.game_keyword",
            targetColumn = "keyword_id",
            materializedGameIds = materializedGameIds,
            sourceRows = gameKeywordRows,
            availableGameIds = availableGameIds,
            availableDimensionIds = availableKeywordIds,
        )

        deleteByGameIds("service.game_company", materializedGameIds)
        val resolvedGameCompanyRows = resolveGameCompanyReferences(
            rows = gameCompanyRows,
            availableGameIds = availableGameIds,
            availableCompanyIds = availableCompanyIds,
        )
        batchUpsert(
            """
            INSERT INTO service.game_company
                (game_id, company_id, is_developer, is_publisher, is_porting, is_supporting)
            VALUES (?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            resolvedGameCompanyRows,
        ) { row ->
            setLong(1, row.gameId)
            setLong(2, row.companyId)
            setBoolean(3, row.isDeveloper)
            setBoolean(4, row.isPublisher)
            setBoolean(5, row.isPorting)
            setBoolean(6, row.isSupporting)
        }

        deleteByGameIds("service.game_relation", materializedGameIds)
        val resolvedGameRelationRows = resolveGameRelationReferences(
            rows = gameRelationRows,
            availableGameIds = availableGameIds,
        )
        batchUpsert(
            """
            INSERT INTO service.game_relation (game_id, related_game_id, relation_type)
            VALUES (?, ?, ?)
            """.trimIndent(),
            resolvedGameRelationRows,
        ) { row ->
            setLong(1, row.gameId)
            setLong(2, row.relatedGameId)
            setString(3, row.relationType)
        }
    }

    fun rebuildGameMediaProjections(
        materializedGameIds: Set<Long>,
        coverRows: List<CoverProjectionRow>,
        artworkRows: List<ArtworkProjectionRow>,
        screenshotRows: List<ScreenshotProjectionRow>,
        gameVideoRows: List<GameVideoProjectionRow>,
        websiteRows: List<WebsiteProjectionRow>,
        alternativeNameRows: List<AlternativeNameProjectionRow>,
        availableGameIds: Set<Long> = loadIds("service.game"),
        availableGameLocalizationsById: Map<Long, Long> = loadCurrentGameLocalizationProjectionRows()
            .associate { it.id to it.gameId },
        availableWebsiteTypeIds: Set<Long> = loadIds("service.website_type"),
    ) {
        if (materializedGameIds.isEmpty()) {
            return
        }

        deleteByGameIds("service.cover", materializedGameIds)
        val resolvedCoverRows = resolveCoverReferences(
            rows = coverRows,
            availableGameIds = availableGameIds,
            availableGameLocalizationsById = availableGameLocalizationsById,
        )
        batchUpsert(
            """
            INSERT INTO service.cover
                (id, game_id, game_localization_id, image_id, url, is_main)
            VALUES (?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            resolvedCoverRows,
        ) { row ->
            setLong(1, row.id)
            setLong(2, row.gameId)
            setNullableLong(3, row.gameLocalizationId)
            setNullableString(4, row.imageId)
            setNullableString(5, row.url)
            setBoolean(6, row.isMain)
        }

        rebuildGameMediaProjection(
            tableName = "service.artwork",
            materializedGameIds = materializedGameIds,
            resolvedRows = resolveArtworkReferences(
                rows = artworkRows,
                availableGameIds = availableGameIds,
            ),
            sql = """
                INSERT INTO service.artwork (id, game_id, image_id, url)
                VALUES (?, ?, ?, ?)
            """.trimIndent(),
        ) { row ->
            setLong(1, row.id)
            setLong(2, row.gameId)
            setNullableString(3, row.imageId)
            setNullableString(4, row.url)
        }

        rebuildGameMediaProjection(
            tableName = "service.screenshot",
            materializedGameIds = materializedGameIds,
            resolvedRows = resolveScreenshotReferences(
                rows = screenshotRows,
                availableGameIds = availableGameIds,
            ),
            sql = """
                INSERT INTO service.screenshot (id, game_id, image_id, url)
                VALUES (?, ?, ?, ?)
            """.trimIndent(),
        ) { row ->
            setLong(1, row.id)
            setLong(2, row.gameId)
            setNullableString(3, row.imageId)
            setNullableString(4, row.url)
        }

        rebuildGameMediaProjection(
            tableName = "service.game_video",
            materializedGameIds = materializedGameIds,
            resolvedRows = resolveGameVideoReferences(
                rows = gameVideoRows,
                availableGameIds = availableGameIds,
            ),
            sql = """
                INSERT INTO service.game_video (id, game_id, name, video_id)
                VALUES (?, ?, ?, ?)
            """.trimIndent(),
        ) { row ->
            setLong(1, row.id)
            setLong(2, row.gameId)
            setNullableString(3, row.name)
            setNullableString(4, row.videoId)
        }

        rebuildGameMediaProjection(
            tableName = "service.website",
            materializedGameIds = materializedGameIds,
            resolvedRows = resolveWebsiteReferences(
                rows = websiteRows,
                availableGameIds = availableGameIds,
                availableTypeIds = availableWebsiteTypeIds,
            ),
            sql = """
                INSERT INTO service.website (id, game_id, type_id, url, is_trusted)
                VALUES (?, ?, ?, ?, ?)
            """.trimIndent(),
        ) { row ->
            setLong(1, row.id)
            setLong(2, row.gameId)
            setNullableLong(3, row.typeId)
            setNullableString(4, row.url)
            setBoolean(5, row.isTrusted)
        }

        rebuildGameMediaProjection(
            tableName = "service.alternative_name",
            materializedGameIds = materializedGameIds,
            resolvedRows = resolveAlternativeNameReferences(
                rows = alternativeNameRows,
                availableGameIds = availableGameIds,
            ),
            sql = """
                INSERT INTO service.alternative_name (id, game_id, name, comment)
                VALUES (?, ?, ?, ?)
            """.trimIndent(),
        ) { row ->
            setLong(1, row.id)
            setLong(2, row.gameId)
            setNullableString(3, row.name)
            setNullableString(4, row.comment)
        }
    }

    private fun syncNamedDimensionByDiff(
        sourceRows: List<NamedDimensionRow>,
        targetTable: String,
        targetValueColumn: String,
    ): ServiceEtlTableSyncResult {
        val existingById = jdbc.query(
            """
            SELECT id, $targetValueColumn AS value
            FROM service.$targetTable
            """.trimIndent(),
        ) { rs, _ ->
            NamedDimensionRow(
                id = rs.getLong("id"),
                value = rs.getString("value"),
            )
        }.associateBy { it.id }
        val changedRows = sourceRows.filter { existingById[it.id] != it }

        batchUpsert(
            """
            INSERT INTO service.$targetTable (id, $targetValueColumn)
            VALUES (?, ?)
            ON CONFLICT (id) DO UPDATE SET
                $targetValueColumn = EXCLUDED.$targetValueColumn
            """.trimIndent(),
            changedRows,
        ) { row ->
            setLong(1, row.id)
            setNullableString(2, row.value)
        }
        return diffResult(changedRows.size)
    }

    private fun loadCurrentLanguagesById(): Map<Long, LanguageRow> =
        jdbc.query(
            """
            SELECT id, locale, name, native_name
            FROM service.language
            """.trimIndent(),
        ) { rs, _ ->
            LanguageRow(
                id = rs.getLong("id"),
                locale = rs.getString("locale"),
                name = rs.getString("name"),
                nativeName = rs.getString("native_name"),
            )
        }.associateBy { it.id }

    private fun loadCurrentRegionsById(): Map<Long, RegionRow> =
        jdbc.query(
            """
            SELECT id, name, identifier
            FROM service.region
            """.trimIndent(),
        ) { rs, _ ->
            RegionRow(
                id = rs.getLong("id"),
                name = rs.getString("name"),
                identifier = rs.getString("identifier"),
            )
        }.associateBy { it.id }

    private fun loadCurrentReleaseStatusesById(): Map<Long, ReleaseStatusRow> =
        jdbc.query(
            """
            SELECT id, name, description
            FROM service.release_status
            """.trimIndent(),
        ) { rs, _ ->
            ReleaseStatusRow(
                id = rs.getLong("id"),
                name = rs.getString("name"),
                description = rs.getString("description"),
            )
        }.associateBy { it.id }

    private fun loadCurrentPlatformLogosById(): Map<Long, PlatformLogoRow> =
        jdbc.query(
            """
            SELECT id, image_id, url
            FROM service.platform_logo
            """.trimIndent(),
        ) { rs, _ ->
            PlatformLogoRow(
                id = rs.getLong("id"),
                imageId = rs.getString("image_id"),
                url = rs.getString("url"),
            )
        }.associateBy { it.id }

    private fun loadCurrentPlatformsById(): Map<Long, PlatformSyncRow> =
        jdbc.query(
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

    private fun loadCurrentCompaniesById(): Map<Long, CompanySyncRow> =
        jdbc.query(
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

    private fun deleteByGameIds(tableName: String, gameIds: Set<Long>) {
        if (gameIds.isEmpty()) {
            return
        }
        gameIds.toList().chunked(PROJECTION_QUERY_CHUNK_SIZE).forEach { chunk ->
            val placeholders = chunk.joinToString(",") { "?" }
            jdbc.update(
                "DELETE FROM $tableName WHERE game_id IN ($placeholders)",
                *chunk.toTypedArray(),
            )
        }
    }

    private fun rebuildGameArrayBridgeProjection(
        tableName: String,
        targetColumn: String,
        materializedGameIds: Set<Long>,
        sourceRows: List<GameDimensionProjectionRow>,
        availableGameIds: Set<Long>,
        availableDimensionIds: Set<Long>,
    ) {
        deleteByGameIds(tableName, materializedGameIds)
        val resolvedRows = resolveGameDimensionReferences(
            rows = sourceRows,
            availableGameIds = availableGameIds,
            availableDimensionIds = availableDimensionIds,
        )
        batchUpsert(
            """
            INSERT INTO $tableName (game_id, $targetColumn)
            VALUES (?, ?)
            """.trimIndent(),
            resolvedRows,
        ) { row ->
            setLong(1, row.gameId)
            setLong(2, row.dimensionId)
        }
    }

    private fun <T> rebuildGameMediaProjection(
        tableName: String,
        materializedGameIds: Set<Long>,
        resolvedRows: List<T>,
        sql: String,
        setter: PreparedStatement.(T) -> Unit,
    ) {
        deleteByGameIds(tableName, materializedGameIds)
        batchUpsert(
            sql = sql,
            rows = resolvedRows,
            setter = setter,
        )
    }

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

    private fun PreparedStatement.setNullableInt(index: Int, value: Int?) {
        if (value != null) {
            setInt(index, value)
        } else {
            setNull(index, Types.INTEGER)
        }
    }

    private fun PreparedStatement.setNullableInstantFromEpochSecond(index: Int, value: Long?) {
        if (value != null) {
            setTimestamp(index, Timestamp.from(Instant.ofEpochSecond(value)))
        } else {
            setNull(index, Types.TIMESTAMP_WITH_TIMEZONE)
        }
    }

    private fun PreparedStatement.setNullableLongArray(index: Int, value: List<Long>?) {
        if (value != null) {
            setArray(
                index,
                connection.createArrayOf("BIGINT", value.map { java.lang.Long.valueOf(it) }.toTypedArray()),
            )
        } else {
            setNull(index, Types.ARRAY)
        }
    }

    private fun PreparedStatement.setNullableString(index: Int, value: String?) {
        if (value != null) {
            setString(index, value)
        } else {
            setNull(index, Types.VARCHAR)
        }
    }

    private fun ResultSet.getNullableLongList(columnName: String): List<Long>? =
        getArray(columnName)
            ?.array
            ?.let { raw ->
                when (raw) {
                    is Array<*> -> raw.mapNotNull { (it as? Number)?.toLong() }
                    else -> null
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

data class ServiceEtlMismatchLogEntry(
    val runId: UUID,
    val tableName: String?,
    val mismatchType: String,
    val expectedValue: String?,
    val actualValue: String?,
    val detailsJson: String?,
    val recordedAt: Instant,
)

data class ServiceEtlTableSyncResult(
    val processedRows: Int,
    val nextCursor: Long?,
    val note: String? = null,
)

data class NamedDimensionRow(
    val id: Long,
    val value: String?,
)

data class LanguageRow(
    val id: Long,
    val locale: String?,
    val name: String?,
    val nativeName: String?,
)

data class RegionRow(
    val id: Long,
    val name: String?,
    val identifier: String?,
)

data class ReleaseStatusRow(
    val id: Long,
    val name: String?,
    val description: String?,
)

data class PlatformLogoRow(
    val id: Long,
    val imageId: String?,
    val url: String?,
)

data class GameProjectionRow(
    val id: Long,
    val slug: String?,
    val name: String?,
    val summary: String?,
    val storyline: String?,
    val firstReleaseDateEpochSecond: Long?,
    val statusId: Long?,
    val typeId: Long?,
    val sourceUpdatedAtEpochSecond: Long?,
    val tags: List<Long>?,
)

data class GameLocalizationProjectionRow(
    val id: Long,
    val gameId: Long,
    val regionId: Long?,
    val name: String?,
)

data class GameReleaseProjectionRow(
    val id: Long,
    val gameId: Long,
    val platformId: Long?,
    val regionId: Long?,
    val statusId: Long?,
    val releaseDateEpochSecond: Long?,
    val year: Int?,
    val month: Int?,
    val dateHuman: String?,
)

data class GameLanguageProjectionRow(
    val gameId: Long,
    val languageId: Long,
    val supportsAudio: Boolean,
    val supportsSubtitles: Boolean,
    val supportsInterface: Boolean,
)

data class GameDimensionProjectionRow(
    val gameId: Long,
    val dimensionId: Long,
)

data class GameCompanyProjectionRow(
    val gameId: Long,
    val companyId: Long,
    val isDeveloper: Boolean,
    val isPublisher: Boolean,
    val isPorting: Boolean,
    val isSupporting: Boolean,
)

data class GameRelationProjectionRow(
    val gameId: Long,
    val relatedGameId: Long,
    val relationType: String,
)

data class CoverProjectionRow(
    val id: Long,
    val gameId: Long,
    val gameLocalizationId: Long?,
    val imageId: String?,
    val url: String?,
    val isMain: Boolean,
)

data class ArtworkProjectionRow(
    val id: Long,
    val gameId: Long,
    val imageId: String?,
    val url: String?,
)

data class ScreenshotProjectionRow(
    val id: Long,
    val gameId: Long,
    val imageId: String?,
    val url: String?,
)

data class GameVideoProjectionRow(
    val id: Long,
    val gameId: Long,
    val name: String?,
    val videoId: String?,
)

data class WebsiteProjectionRow(
    val id: Long,
    val gameId: Long,
    val typeId: Long?,
    val url: String?,
    val isTrusted: Boolean,
)

data class AlternativeNameProjectionRow(
    val id: Long,
    val gameId: Long,
    val name: String?,
    val comment: String?,
)

data class PlatformSyncRow(
    val id: Long,
    val name: String?,
    val abbreviation: String?,
    val alternativeName: String?,
    val logoId: Long?,
    val typeId: Long?,
)

data class CompanySyncRow(
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

internal fun resolveGameReferences(
    rows: List<GameProjectionRow>,
    availableStatusIds: Set<Long>,
    availableTypeIds: Set<Long>,
): List<GameProjectionRow> = rows.map { row ->
    row.copy(
        statusId = row.statusId?.takeIf { it in availableStatusIds },
        typeId = row.typeId?.takeIf { it in availableTypeIds },
    )
}

internal fun resolveGameLocalizationReferences(
    rows: List<GameLocalizationProjectionRow>,
    availableGameIds: Set<Long>,
    availableRegionIds: Set<Long>,
): List<GameLocalizationProjectionRow> = rows.mapNotNull { row ->
    row.takeIf { it.gameId in availableGameIds }?.copy(
        regionId = row.regionId?.takeIf { it in availableRegionIds },
    )
}

internal fun resolveGameReleaseReferences(
    rows: List<GameReleaseProjectionRow>,
    availableGameIds: Set<Long>,
    availablePlatformIds: Set<Long>,
    availableRegionIds: Set<Long>,
    availableStatusIds: Set<Long>,
): List<GameReleaseProjectionRow> = rows.mapNotNull { row ->
    row.takeIf { it.gameId in availableGameIds }?.copy(
        platformId = row.platformId?.takeIf { it in availablePlatformIds },
        regionId = row.regionId?.takeIf { it in availableRegionIds },
        statusId = row.statusId?.takeIf { it in availableStatusIds },
    )
}

internal fun resolveGameLanguageReferences(
    rows: List<GameLanguageProjectionRow>,
    availableGameIds: Set<Long>,
    availableLanguageIds: Set<Long>,
): List<GameLanguageProjectionRow> = rows.mapNotNull { row ->
    row.takeIf {
        it.gameId in availableGameIds &&
            it.languageId in availableLanguageIds &&
            (it.supportsAudio || it.supportsSubtitles || it.supportsInterface)
    }
}

internal fun resolveGameDimensionReferences(
    rows: List<GameDimensionProjectionRow>,
    availableGameIds: Set<Long>,
    availableDimensionIds: Set<Long>,
): List<GameDimensionProjectionRow> = rows.mapNotNull { row ->
    row.takeIf { it.gameId in availableGameIds && it.dimensionId in availableDimensionIds }
}

internal fun resolveGameCompanyReferences(
    rows: List<GameCompanyProjectionRow>,
    availableGameIds: Set<Long>,
    availableCompanyIds: Set<Long>,
): List<GameCompanyProjectionRow> = rows.mapNotNull { row ->
    row.takeIf {
        it.gameId in availableGameIds &&
            it.companyId in availableCompanyIds &&
            (it.isDeveloper || it.isPublisher || it.isPorting || it.isSupporting)
    }
}

internal fun resolveGameRelationReferences(
    rows: List<GameRelationProjectionRow>,
    availableGameIds: Set<Long>,
): List<GameRelationProjectionRow> = rows.mapNotNull { row ->
    row.takeIf { it.gameId in availableGameIds && it.relatedGameId in availableGameIds }
}

internal fun resolveCoverReferences(
    rows: List<CoverProjectionRow>,
    availableGameIds: Set<Long>,
    availableGameLocalizationsById: Map<Long, Long>,
): List<CoverProjectionRow> = rows.mapNotNull { row ->
    row.takeIf { it.gameId in availableGameIds }?.copy(
        gameLocalizationId = row.gameLocalizationId?.takeIf { localizationId ->
            availableGameLocalizationsById[localizationId] == row.gameId
        },
    )
}

internal fun resolveArtworkReferences(
    rows: List<ArtworkProjectionRow>,
    availableGameIds: Set<Long>,
): List<ArtworkProjectionRow> = rows.mapNotNull { row ->
    row.takeIf { it.gameId in availableGameIds }
}

internal fun resolveScreenshotReferences(
    rows: List<ScreenshotProjectionRow>,
    availableGameIds: Set<Long>,
): List<ScreenshotProjectionRow> = rows.mapNotNull { row ->
    row.takeIf { it.gameId in availableGameIds }
}

internal fun resolveGameVideoReferences(
    rows: List<GameVideoProjectionRow>,
    availableGameIds: Set<Long>,
): List<GameVideoProjectionRow> = rows.mapNotNull { row ->
    row.takeIf { it.gameId in availableGameIds }
}

internal fun resolveWebsiteReferences(
    rows: List<WebsiteProjectionRow>,
    availableGameIds: Set<Long>,
    availableTypeIds: Set<Long>,
): List<WebsiteProjectionRow> = rows.mapNotNull { row ->
    row.takeIf { it.gameId in availableGameIds }?.copy(
        typeId = row.typeId?.takeIf { it in availableTypeIds },
    )
}

internal fun resolveAlternativeNameReferences(
    rows: List<AlternativeNameProjectionRow>,
    availableGameIds: Set<Long>,
): List<AlternativeNameProjectionRow> = rows.mapNotNull { row ->
    row.takeIf { it.gameId in availableGameIds }
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
