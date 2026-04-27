package com.projectgc.calendar.repository.etl

import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet

@Repository
class IngestEtlReadJdbcRepository(
    @Qualifier("ingestReadJdbcTemplate")
    private val jdbc: JdbcTemplate,
) {
    companion object {
        private const val PROJECTION_QUERY_CHUNK_SIZE = 500
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

    fun loadGameStatuses(): List<NamedDimensionRow> =
        loadNamedDimensionRows(
            sourceTable = "game_status",
            sourceValueColumn = "status",
        )

    fun loadGameTypes(): List<NamedDimensionRow> =
        loadNamedDimensionRows(
            sourceTable = "game_type",
            sourceValueColumn = "type",
        )

    fun loadLanguages(): List<LanguageRow> =
        jdbc.query(
            """
            SELECT id, locale, name, native_name
            FROM ingest.language
            ORDER BY id
            """.trimIndent(),
        ) { rs, _ ->
            LanguageRow(
                id = rs.getLong("id"),
                locale = rs.getString("locale"),
                name = rs.getString("name"),
                nativeName = rs.getString("native_name"),
            )
        }

    fun loadRegions(): List<RegionRow> =
        jdbc.query(
            """
            SELECT id, name, identifier
            FROM ingest.region
            ORDER BY id
            """.trimIndent(),
        ) { rs, _ ->
            RegionRow(
                id = rs.getLong("id"),
                name = rs.getString("name"),
                identifier = rs.getString("identifier"),
            )
        }

    fun loadReleaseRegions(): List<NamedDimensionRow> =
        loadNamedDimensionRows(
            sourceTable = "release_date_region",
            sourceValueColumn = "region",
        )

    fun loadReleaseStatuses(): List<ReleaseStatusRow> =
        jdbc.query(
            """
            SELECT id, name, description
            FROM ingest.release_date_status
            ORDER BY id
            """.trimIndent(),
        ) { rs, _ ->
            ReleaseStatusRow(
                id = rs.getLong("id"),
                name = rs.getString("name"),
                description = rs.getString("description"),
            )
        }

    fun loadGenres(): List<NamedDimensionRow> =
        loadNamedDimensionRows(
            sourceTable = "genre",
            sourceValueColumn = "name",
        )

    fun loadThemes(): List<NamedDimensionRow> =
        loadNamedDimensionRows(
            sourceTable = "theme",
            sourceValueColumn = "name",
        )

    fun loadPlayerPerspectives(): List<NamedDimensionRow> =
        loadNamedDimensionRows(
            sourceTable = "player_perspective",
            sourceValueColumn = "name",
        )

    fun loadGameModes(): List<NamedDimensionRow> =
        loadNamedDimensionRows(
            sourceTable = "game_mode",
            sourceValueColumn = "name",
        )

    fun loadKeywords(): List<NamedDimensionRow> =
        loadNamedDimensionRows(
            sourceTable = "keyword",
            sourceValueColumn = "name",
        )

    fun loadLanguageSupportTypes(): List<NamedDimensionRow> =
        loadNamedDimensionRows(
            sourceTable = "language_support_type",
            sourceValueColumn = "name",
        )

    fun loadWebsiteTypes(): List<NamedDimensionRow> =
        loadNamedDimensionRows(
            sourceTable = "website_type",
            sourceValueColumn = "type",
        )

    fun loadPlatformLogos(): List<PlatformLogoRow> =
        jdbc.query(
            """
            SELECT id, image_id, url
            FROM ingest.platform_logo
            ORDER BY id
            """.trimIndent(),
        ) { rs, _ ->
            PlatformLogoRow(
                id = rs.getLong("id"),
                imageId = rs.getString("image_id"),
                url = rs.getString("url"),
            )
        }

    fun loadPlatformTypes(): List<NamedDimensionRow> =
        loadNamedDimensionRows(
            sourceTable = "platform_type",
            sourceValueColumn = "name",
        )

    fun loadPlatforms(): List<PlatformSyncRow> =
        jdbc.query(
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

    fun loadCompanies(): List<CompanySyncRow> =
        jdbc.query(
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

    fun loadAllGameProjectionRows(): List<GameProjectionRow> =
        jdbc.query(
            """
            SELECT id, slug, name, summary, storyline, first_release_date, game_status, game_type, updated_at, tags
            FROM ingest.game
            ORDER BY id
            """.trimIndent(),
        ) { rs, _ -> rs.toGameProjectionRow() }

    fun loadGameProjectionRows(gameIds: Set<Long>): List<GameProjectionRow> =
        queryByLongIdChunks(
            ids = gameIds,
            sqlBuilder = { placeholders ->
                """
                SELECT id, slug, name, summary, storyline, first_release_date, game_status, game_type, updated_at, tags
                FROM ingest.game
                WHERE id IN ($placeholders)
                ORDER BY id
                """.trimIndent()
            },
            rowMapper = { rs, _ -> rs.toGameProjectionRow() },
        )

    fun loadAllGameLocalizationProjectionRows(): List<GameLocalizationProjectionRow> =
        jdbc.query(
            """
            SELECT id, game, region, name
            FROM ingest.game_localization
            WHERE game IS NOT NULL
            ORDER BY game, id
            """.trimIndent(),
        ) { rs, _ -> rs.toGameLocalizationProjectionRow() }

    fun loadGameLocalizationProjectionRows(gameIds: Set<Long>): List<GameLocalizationProjectionRow> =
        queryByLongIdChunks(
            ids = gameIds,
            sqlBuilder = { placeholders ->
                """
                SELECT id, game, region, name
                FROM ingest.game_localization
                WHERE game IN ($placeholders)
                ORDER BY game, id
                """.trimIndent()
            },
            rowMapper = { rs, _ -> rs.toGameLocalizationProjectionRow() },
        )

    fun loadAllGameReleaseProjectionRows(): List<GameReleaseProjectionRow> =
        jdbc.query(
            """
            SELECT id, game, platform, release_region, status, date, y, m, human
            FROM ingest.release_date
            WHERE game IS NOT NULL
            ORDER BY game, id
            """.trimIndent(),
        ) { rs, _ -> rs.toGameReleaseProjectionRow() }

    fun loadGameReleaseProjectionRows(gameIds: Set<Long>): List<GameReleaseProjectionRow> =
        queryByLongIdChunks(
            ids = gameIds,
            sqlBuilder = { placeholders ->
                """
                SELECT id, game, platform, release_region, status, date, y, m, human
                FROM ingest.release_date
                WHERE game IN ($placeholders)
                ORDER BY game, id
                """.trimIndent()
            },
            rowMapper = { rs, _ -> rs.toGameReleaseProjectionRow() },
        )

    fun loadAllGameLanguageProjectionRows(): List<GameLanguageProjectionRow> =
        loadGameLanguageProjectionRowsInternal(null)

    fun loadGameLanguageProjectionRows(gameIds: Set<Long>): List<GameLanguageProjectionRow> =
        loadGameLanguageProjectionRowsInternal(gameIds)

    fun loadAllGameArrayProjectionRows(sourceColumn: String): List<GameDimensionProjectionRow> =
        loadGameArrayProjectionRowsInternal(
            gameIds = null,
            sourceColumn = sourceColumn,
        )

    fun loadGameArrayProjectionRows(
        gameIds: Set<Long>,
        sourceColumn: String,
    ): List<GameDimensionProjectionRow> =
        loadGameArrayProjectionRowsInternal(
            gameIds = gameIds,
            sourceColumn = sourceColumn,
        )

    fun loadAllGameCompanyProjectionRows(): List<GameCompanyProjectionRow> =
        loadGameCompanyProjectionRowsInternal(null)

    fun loadGameCompanyProjectionRows(gameIds: Set<Long>): List<GameCompanyProjectionRow> =
        loadGameCompanyProjectionRowsInternal(gameIds)

    fun loadAllGameRelationProjectionRows(): List<GameRelationProjectionRow> =
        loadGameRelationProjectionRowsInternal(null)

    fun loadGameRelationProjectionRows(gameIds: Set<Long>): List<GameRelationProjectionRow> =
        loadGameRelationProjectionRowsInternal(gameIds)

    fun loadAllCoverProjectionRows(): List<CoverProjectionRow> =
        loadCoverProjectionRowsInternal(null)

    fun loadCoverProjectionRows(gameIds: Set<Long>): List<CoverProjectionRow> =
        loadCoverProjectionRowsInternal(gameIds)

    fun loadAllArtworkProjectionRows(): List<ArtworkProjectionRow> =
        loadArtworkProjectionRowsInternal(null)

    fun loadArtworkProjectionRows(gameIds: Set<Long>): List<ArtworkProjectionRow> =
        loadArtworkProjectionRowsInternal(gameIds)

    fun loadAllScreenshotProjectionRows(): List<ScreenshotProjectionRow> =
        loadScreenshotProjectionRowsInternal(null)

    fun loadScreenshotProjectionRows(gameIds: Set<Long>): List<ScreenshotProjectionRow> =
        loadScreenshotProjectionRowsInternal(gameIds)

    fun loadAllGameVideoProjectionRows(): List<GameVideoProjectionRow> =
        loadGameVideoProjectionRowsInternal(null)

    fun loadGameVideoProjectionRows(gameIds: Set<Long>): List<GameVideoProjectionRow> =
        loadGameVideoProjectionRowsInternal(gameIds)

    fun loadAllWebsiteProjectionRows(): List<WebsiteProjectionRow> =
        loadWebsiteProjectionRowsInternal(null)

    fun loadWebsiteProjectionRows(gameIds: Set<Long>): List<WebsiteProjectionRow> =
        loadWebsiteProjectionRowsInternal(gameIds)

    fun loadAllAlternativeNameProjectionRows(): List<AlternativeNameProjectionRow> =
        loadAlternativeNameProjectionRowsInternal(null)

    fun loadAlternativeNameProjectionRows(gameIds: Set<Long>): List<AlternativeNameProjectionRow> =
        loadAlternativeNameProjectionRowsInternal(gameIds)

    private fun loadNamedDimensionRows(
        sourceTable: String,
        sourceValueColumn: String,
    ): List<NamedDimensionRow> =
        jdbc.query(
            """
            SELECT id, $sourceValueColumn AS value
            FROM ingest.$sourceTable
            ORDER BY id
            """.trimIndent(),
        ) { rs, _ ->
            NamedDimensionRow(
                id = rs.getLong("id"),
                value = rs.getString("value"),
            )
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

    private fun loadGameLanguageProjectionRowsInternal(gameIds: Set<Long>?): List<GameLanguageProjectionRow> =
        queryByOptionalGameIds(
            gameIds = gameIds,
            sqlBuilder = { filterClause ->
                """
                SELECT
                    ls.game AS game_id,
                    ls.language AS language_id,
                    bool_or(lower(COALESCE(lst.name, '')) = 'audio') AS supports_audio,
                    bool_or(lower(COALESCE(lst.name, '')) IN ('subtitles', 'subtitle')) AS supports_subtitles,
                    bool_or(lower(COALESCE(lst.name, '')) = 'interface') AS supports_interface
                FROM ingest.language_support ls
                LEFT JOIN ingest.language_support_type lst ON lst.id = ls.language_support_type
                WHERE ls.language IS NOT NULL
                  $filterClause
                GROUP BY ls.game, ls.language
                HAVING bool_or(lower(COALESCE(lst.name, '')) = 'audio')
                    OR bool_or(lower(COALESCE(lst.name, '')) IN ('subtitles', 'subtitle'))
                    OR bool_or(lower(COALESCE(lst.name, '')) = 'interface')
                ORDER BY ls.game, ls.language
                """.trimIndent()
            },
        ) { rs, _ ->
            GameLanguageProjectionRow(
                gameId = rs.getLong("game_id"),
                languageId = rs.getLong("language_id"),
                supportsAudio = rs.getBoolean("supports_audio"),
                supportsSubtitles = rs.getBoolean("supports_subtitles"),
                supportsInterface = rs.getBoolean("supports_interface"),
            )
        }

    private fun loadGameArrayProjectionRowsInternal(
        gameIds: Set<Long>?,
        sourceColumn: String,
    ): List<GameDimensionProjectionRow> =
        queryByOptionalGameIds(
            gameIds = gameIds,
            filterColumn = "id",
            sqlBuilder = { filterClause ->
                """
                WITH filtered_games AS (
                    SELECT id, $sourceColumn
                    FROM ingest.game
                    WHERE 1 = 1
                      $filterClause
                )
                SELECT DISTINCT game_id, dimension_id
                FROM (
                    SELECT fg.id AS game_id, ref.dimension_id
                    FROM filtered_games fg
                    CROSS JOIN LATERAL unnest(COALESCE(fg.$sourceColumn, ARRAY[]::BIGINT[])) AS ref(dimension_id)
                ) rows
                ORDER BY game_id, dimension_id
                """.trimIndent()
            },
        ) { rs, _ ->
            GameDimensionProjectionRow(
                gameId = rs.getLong("game_id"),
                dimensionId = rs.getLong("dimension_id"),
            )
        }

    private fun loadGameCompanyProjectionRowsInternal(gameIds: Set<Long>?): List<GameCompanyProjectionRow> =
        queryByOptionalGameIds(
            gameIds = gameIds,
            sqlBuilder = { filterClause ->
                """
                SELECT
                    ic.game AS game_id,
                    ic.company AS company_id,
                    bool_or(COALESCE(ic.developer, FALSE)) AS is_developer,
                    bool_or(COALESCE(ic.publisher, FALSE)) AS is_publisher,
                    bool_or(COALESCE(ic.porting, FALSE)) AS is_porting,
                    bool_or(COALESCE(ic.supporting, FALSE)) AS is_supporting
                FROM ingest.involved_company ic
                WHERE ic.company IS NOT NULL
                  $filterClause
                GROUP BY ic.game, ic.company
                HAVING bool_or(COALESCE(ic.developer, FALSE))
                    OR bool_or(COALESCE(ic.publisher, FALSE))
                    OR bool_or(COALESCE(ic.porting, FALSE))
                    OR bool_or(COALESCE(ic.supporting, FALSE))
                ORDER BY ic.game, ic.company
                """.trimIndent()
            },
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

    private fun loadGameRelationProjectionRowsInternal(gameIds: Set<Long>?): List<GameRelationProjectionRow> =
        queryByOptionalGameIds(
            gameIds = gameIds,
            filterColumn = "id",
            sqlBuilder = { filterClause ->
                """
                WITH filtered_games AS (
                    SELECT
                        id,
                        parent_game,
                        remakes,
                        remasters,
                        ports,
                        standalone_expansions,
                        similar_games
                    FROM ingest.game
                    WHERE 1 = 1
                      $filterClause
                )
                SELECT game_id, related_game_id, relation_type
                FROM (
                    SELECT id AS game_id, parent_game AS related_game_id, 'PARENT' AS relation_type
                    FROM filtered_games
                    WHERE parent_game IS NOT NULL
                    UNION
                    SELECT fg.id AS game_id, rel.related_game_id, 'REMAKE' AS relation_type
                    FROM filtered_games fg
                    CROSS JOIN LATERAL unnest(COALESCE(fg.remakes, ARRAY[]::BIGINT[])) AS rel(related_game_id)
                    UNION
                    SELECT fg.id AS game_id, rel.related_game_id, 'REMASTER' AS relation_type
                    FROM filtered_games fg
                    CROSS JOIN LATERAL unnest(COALESCE(fg.remasters, ARRAY[]::BIGINT[])) AS rel(related_game_id)
                    UNION
                    SELECT fg.id AS game_id, rel.related_game_id, 'PORT' AS relation_type
                    FROM filtered_games fg
                    CROSS JOIN LATERAL unnest(COALESCE(fg.ports, ARRAY[]::BIGINT[])) AS rel(related_game_id)
                    UNION
                    SELECT fg.id AS game_id, rel.related_game_id, 'STANDALONE_EXPANSION' AS relation_type
                    FROM filtered_games fg
                    CROSS JOIN LATERAL unnest(COALESCE(fg.standalone_expansions, ARRAY[]::BIGINT[])) AS rel(related_game_id)
                    UNION
                    SELECT fg.id AS game_id, rel.related_game_id, 'SIMILAR' AS relation_type
                    FROM filtered_games fg
                    CROSS JOIN LATERAL unnest(COALESCE(fg.similar_games, ARRAY[]::BIGINT[])) AS rel(related_game_id)
                ) rows
                ORDER BY game_id, relation_type, related_game_id
                """.trimIndent()
            },
        ) { rs, _ ->
            GameRelationProjectionRow(
                gameId = rs.getLong("game_id"),
                relatedGameId = rs.getLong("related_game_id"),
                relationType = rs.getString("relation_type"),
            )
        }

    private fun loadCoverProjectionRowsInternal(gameIds: Set<Long>?): List<CoverProjectionRow> =
        if (gameIds == null) {
            jdbc.query(
                """
                WITH cover_rows AS (
                    SELECT
                        c.id,
                        COALESCE(c.game, gl.game) AS game_id,
                        c.game_localization AS game_localization_id,
                        c.image_id,
                        c.url,
                        COALESCE(g.cover = c.id, FALSE) AS is_main
                    FROM ingest.cover c
                    LEFT JOIN ingest.game_localization gl ON gl.id = c.game_localization
                    LEFT JOIN ingest.game g ON g.id = COALESCE(c.game, gl.game)
                )
                SELECT id, game_id, game_localization_id, image_id, url, is_main
                FROM cover_rows
                WHERE game_id IS NOT NULL
                ORDER BY game_id, id
                """.trimIndent(),
            ) { rs, _ -> rs.toCoverProjectionRow() }
        } else {
            queryByLongIdChunks(
                ids = gameIds,
                sqlBuilder = { placeholders ->
                    """
                    WITH cover_rows AS (
                        SELECT
                            c.id,
                            COALESCE(c.game, gl.game) AS game_id,
                            c.game_localization AS game_localization_id,
                            c.image_id,
                            c.url,
                            COALESCE(g.cover = c.id, FALSE) AS is_main
                        FROM ingest.cover c
                        LEFT JOIN ingest.game_localization gl ON gl.id = c.game_localization
                        LEFT JOIN ingest.game g ON g.id = COALESCE(c.game, gl.game)
                    )
                    SELECT id, game_id, game_localization_id, image_id, url, is_main
                    FROM cover_rows
                    WHERE game_id IN ($placeholders)
                    ORDER BY game_id, id
                    """.trimIndent()
                },
                rowMapper = { rs, _ -> rs.toCoverProjectionRow() },
            )
        }

    private fun loadArtworkProjectionRowsInternal(gameIds: Set<Long>?): List<ArtworkProjectionRow> =
        queryByOptionalGameIds(
            gameIds = gameIds,
            sqlBuilder = { filterClause ->
                """
                SELECT id, game AS game_id, image_id, url
                FROM ingest.artwork
                WHERE game IS NOT NULL
                  $filterClause
                ORDER BY game, id
                """.trimIndent()
            },
        ) { rs, _ -> rs.toArtworkProjectionRow() }

    private fun loadScreenshotProjectionRowsInternal(gameIds: Set<Long>?): List<ScreenshotProjectionRow> =
        queryByOptionalGameIds(
            gameIds = gameIds,
            sqlBuilder = { filterClause ->
                """
                SELECT id, game AS game_id, image_id, url
                FROM ingest.screenshot
                WHERE game IS NOT NULL
                  $filterClause
                ORDER BY game, id
                """.trimIndent()
            },
        ) { rs, _ -> rs.toScreenshotProjectionRow() }

    private fun loadGameVideoProjectionRowsInternal(gameIds: Set<Long>?): List<GameVideoProjectionRow> =
        queryByOptionalGameIds(
            gameIds = gameIds,
            sqlBuilder = { filterClause ->
                """
                SELECT id, game AS game_id, name, video_id
                FROM ingest.game_video
                WHERE game IS NOT NULL
                  $filterClause
                ORDER BY game, id
                """.trimIndent()
            },
        ) { rs, _ -> rs.toGameVideoProjectionRow() }

    private fun loadWebsiteProjectionRowsInternal(gameIds: Set<Long>?): List<WebsiteProjectionRow> =
        queryByOptionalGameIds(
            gameIds = gameIds,
            sqlBuilder = { filterClause ->
                """
                SELECT id, game AS game_id, type AS type_id, url, COALESCE(trusted, FALSE) AS is_trusted
                FROM ingest.website
                WHERE game IS NOT NULL
                  $filterClause
                ORDER BY game, id
                """.trimIndent()
            },
        ) { rs, _ -> rs.toWebsiteProjectionRow() }

    private fun loadAlternativeNameProjectionRowsInternal(gameIds: Set<Long>?): List<AlternativeNameProjectionRow> =
        queryByOptionalGameIds(
            gameIds = gameIds,
            sqlBuilder = { filterClause ->
                """
                SELECT id, game AS game_id, name, comment
                FROM ingest.alternative_name
                WHERE game IS NOT NULL
                  $filterClause
                ORDER BY game, id
                """.trimIndent()
            },
        ) { rs, _ -> rs.toAlternativeNameProjectionRow() }

    private fun ResultSet.toGameProjectionRow() =
        GameProjectionRow(
            id = getLong("id"),
            slug = getString("slug"),
            name = getString("name"),
            summary = getString("summary"),
            storyline = getString("storyline"),
            firstReleaseDateEpochSecond = getLong("first_release_date").takeIf { !wasNull() },
            statusId = getLong("game_status").takeIf { !wasNull() },
            typeId = getLong("game_type").takeIf { !wasNull() },
            sourceUpdatedAtEpochSecond = getLong("updated_at").takeIf { !wasNull() },
            tags = getNullableLongList("tags"),
        )

    private fun ResultSet.toGameLocalizationProjectionRow() =
        GameLocalizationProjectionRow(
            id = getLong("id"),
            gameId = getLong("game"),
            regionId = getLong("region").takeIf { !wasNull() },
            name = getString("name"),
        )

    private fun ResultSet.toGameReleaseProjectionRow() =
        GameReleaseProjectionRow(
            id = getLong("id"),
            gameId = getLong("game"),
            platformId = getLong("platform").takeIf { !wasNull() },
            regionId = getLong("release_region").takeIf { !wasNull() },
            statusId = getLong("status").takeIf { !wasNull() },
            releaseDateEpochSecond = getLong("date").takeIf { !wasNull() },
            year = getInt("y").takeIf { !wasNull() },
            month = getInt("m").takeIf { !wasNull() },
            dateHuman = getString("human"),
        )

    private fun ResultSet.toCoverProjectionRow() =
        CoverProjectionRow(
            id = getLong("id"),
            gameId = getLong("game_id"),
            gameLocalizationId = getLong("game_localization_id").takeIf { !wasNull() },
            imageId = getString("image_id"),
            url = getString("url"),
            isMain = getBoolean("is_main"),
        )

    private fun ResultSet.toArtworkProjectionRow() =
        ArtworkProjectionRow(
            id = getLong("id"),
            gameId = getLong("game_id"),
            imageId = getString("image_id"),
            url = getString("url"),
        )

    private fun ResultSet.toScreenshotProjectionRow() =
        ScreenshotProjectionRow(
            id = getLong("id"),
            gameId = getLong("game_id"),
            imageId = getString("image_id"),
            url = getString("url"),
        )

    private fun ResultSet.toGameVideoProjectionRow() =
        GameVideoProjectionRow(
            id = getLong("id"),
            gameId = getLong("game_id"),
            name = getString("name"),
            videoId = getString("video_id"),
        )

    private fun ResultSet.toWebsiteProjectionRow() =
        WebsiteProjectionRow(
            id = getLong("id"),
            gameId = getLong("game_id"),
            typeId = getLong("type_id").takeIf { !wasNull() },
            url = getString("url"),
            isTrusted = getBoolean("is_trusted"),
        )

    private fun ResultSet.toAlternativeNameProjectionRow() =
        AlternativeNameProjectionRow(
            id = getLong("id"),
            gameId = getLong("game_id"),
            name = getString("name"),
            comment = getString("comment"),
        )

    private fun <T> queryByOptionalGameIds(
        gameIds: Set<Long>?,
        filterColumn: String = "game",
        sqlBuilder: (String) -> String,
        rowMapper: (ResultSet, Int) -> T,
    ): List<T> =
        if (gameIds == null) {
            jdbc.query(sqlBuilder(""), rowMapper)
        } else {
            queryByLongIdChunks(
                ids = gameIds,
                sqlBuilder = { placeholders -> sqlBuilder("AND $filterColumn IN ($placeholders)") },
                rowMapper = rowMapper,
            )
        }

    private fun <T> queryByLongIdChunks(
        ids: Set<Long>,
        sqlBuilder: (String) -> String,
        rowMapper: (ResultSet, Int) -> T,
    ): List<T> {
        if (ids.isEmpty()) {
            return emptyList()
        }
        val rows = mutableListOf<T>()
        ids.toList().chunked(PROJECTION_QUERY_CHUNK_SIZE).forEach { chunk ->
            val placeholders = chunk.joinToString(",") { "?" }
            rows += jdbc.query(
                sqlBuilder(placeholders),
                rowMapper,
                *chunk.toTypedArray(),
            )
        }
        return rows
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
