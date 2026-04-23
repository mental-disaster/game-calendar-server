package com.projectgc.calendar.repository.etl

import com.projectgc.calendar.service.etl.AffectedGameIdCalculator
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class ServiceEtlJdbcRepositoryIntegrationTest {

    companion object {
        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
    }

    private lateinit var ingestJdbc: JdbcTemplate
    private lateinit var serviceJdbc: JdbcTemplate
    private lateinit var ingestRepository: IngestEtlReadJdbcRepository
    private lateinit var serviceRepository: ServiceEtlJdbcRepository
    private lateinit var calculator: AffectedGameIdCalculator

    @BeforeEach
    fun setUp() {
        val ingestDataSource = DriverManagerDataSource(
            postgres.jdbcUrl,
            postgres.username,
            postgres.password,
        )
        val serviceDataSource = DriverManagerDataSource(
            postgres.jdbcUrl,
            postgres.username,
            postgres.password,
        )
        ingestJdbc = JdbcTemplate(ingestDataSource)
        serviceJdbc = JdbcTemplate(serviceDataSource)
        ingestRepository = IngestEtlReadJdbcRepository(ingestJdbc)
        serviceRepository = ServiceEtlJdbcRepository(serviceJdbc)
        calculator = AffectedGameIdCalculator(ingestRepository, serviceRepository)
        resetSchemas()
    }

    @Test
    fun `game relation diff includes existing source game when related game is newly materialized in same run`() {
        ingestJdbc.update("INSERT INTO ingest.game (id, similar_games) VALUES (1, ARRAY[2]::BIGINT[])")
        ingestJdbc.update("INSERT INTO ingest.game (id) VALUES (2)")
        serviceJdbc.update("INSERT INTO service.game (id) VALUES (1)")

        val calculationResult = calculator.calculate(500L)
        val gameSourceResult = calculationResult.sourceResults.first { it.tableName == "game" }

        assertTrue(1L in gameSourceResult.affectedGameIds)
    }

    @Test
    fun `rebuildGameDependentBridgeProjections inserts game relation for related game materialized in same run`() {
        ingestJdbc.update("INSERT INTO ingest.game (id, similar_games) VALUES (1, ARRAY[2]::BIGINT[])")
        ingestJdbc.update("INSERT INTO ingest.game (id) VALUES (2)")
        serviceJdbc.update("INSERT INTO service.game (id) VALUES (1)")
        serviceJdbc.update("INSERT INTO service.game (id) VALUES (2)")
        serviceJdbc.update("INSERT INTO service.game_relation (game_id, related_game_id, relation_type) VALUES (1, 99, 'SIMILAR')")

        serviceRepository.rebuildGameDependentBridgeProjections(
            materializedGameIds = linkedSetOf(1L, 2L),
            gameLanguageRows = emptyList(),
            gameGenreRows = emptyList(),
            gameThemeRows = emptyList(),
            gamePlayerPerspectiveRows = emptyList(),
            gameModeRows = emptyList(),
            gameKeywordRows = emptyList(),
            gameCompanyRows = emptyList(),
            gameRelationRows = ingestRepository.loadGameRelationProjectionRows(linkedSetOf(1L, 2L)),
        )

        val relationRows = serviceJdbc.query(
            """
            SELECT game_id, related_game_id, relation_type
            FROM service.game_relation
            ORDER BY game_id, related_game_id, relation_type
            """.trimIndent(),
        ) { rs, _ ->
            Triple(
                rs.getLong("game_id"),
                rs.getLong("related_game_id"),
                rs.getString("relation_type"),
            )
        }

        assertEquals(
            listOf(Triple(1L, 2L, "SIMILAR")),
            relationRows,
        )
    }

    private fun resetSchemas() {
        executeAll(
            "DROP SCHEMA IF EXISTS service CASCADE",
            "DROP SCHEMA IF EXISTS ingest CASCADE",
            "CREATE SCHEMA ingest",
            "CREATE SCHEMA service",
            """
            CREATE TABLE ingest.game (
                id BIGINT PRIMARY KEY,
                slug TEXT NULL,
                name TEXT NULL,
                summary TEXT NULL,
                storyline TEXT NULL,
                first_release_date BIGINT NULL,
                game_status BIGINT NULL,
                game_type BIGINT NULL,
                updated_at BIGINT NULL,
                tags BIGINT[] NULL,
                parent_game BIGINT NULL,
                remakes BIGINT[] NULL,
                remasters BIGINT[] NULL,
                ports BIGINT[] NULL,
                standalone_expansions BIGINT[] NULL,
                similar_games BIGINT[] NULL,
                genres BIGINT[] NULL,
                themes BIGINT[] NULL,
                player_perspectives BIGINT[] NULL,
                game_modes BIGINT[] NULL,
                keywords BIGINT[] NULL
            )
            """.trimIndent(),
            """
            CREATE TABLE ingest.release_date (
                id BIGINT PRIMARY KEY,
                game BIGINT NULL,
                platform BIGINT NULL,
                release_region BIGINT NULL,
                status BIGINT NULL,
                date BIGINT NULL,
                y INT NULL,
                m INT NULL,
                human TEXT NULL,
                updated_at BIGINT NULL
            )
            """.trimIndent(),
            """
            CREATE TABLE ingest.game_localization (
                id BIGINT PRIMARY KEY,
                game BIGINT NULL,
                region BIGINT NULL,
                name TEXT NULL,
                updated_at BIGINT NULL
            )
            """.trimIndent(),
            """
            CREATE TABLE ingest.language_support_type (
                id BIGINT PRIMARY KEY,
                name TEXT NOT NULL
            )
            """.trimIndent(),
            """
            CREATE TABLE ingest.language_support (
                id BIGINT PRIMARY KEY,
                game BIGINT NOT NULL,
                language BIGINT NOT NULL,
                language_support_type BIGINT NULL,
                updated_at BIGINT NULL
            )
            """.trimIndent(),
            """
            CREATE TABLE ingest.involved_company (
                id BIGINT PRIMARY KEY,
                game BIGINT NOT NULL,
                company BIGINT NOT NULL,
                developer BOOLEAN NULL,
                publisher BOOLEAN NULL,
                porting BOOLEAN NULL,
                supporting BOOLEAN NULL,
                updated_at BIGINT NULL
            )
            """.trimIndent(),
            """
            CREATE TABLE service.game (
                id BIGINT PRIMARY KEY,
                slug TEXT NULL,
                name TEXT NULL,
                summary TEXT NULL,
                storyline TEXT NULL,
                first_release_date TIMESTAMP NULL,
                status_id BIGINT NULL,
                type_id BIGINT NULL,
                source_updated_at TIMESTAMP NULL,
                tags BIGINT[] NULL
            )
            """.trimIndent(),
            """
            CREATE TABLE service.game_release (
                id BIGINT PRIMARY KEY,
                game_id BIGINT NOT NULL,
                platform_id BIGINT NULL,
                region_id BIGINT NULL,
                status_id BIGINT NULL,
                release_date TIMESTAMP NULL,
                year INT NULL,
                month INT NULL,
                date_human TEXT NULL
            )
            """.trimIndent(),
            """
            CREATE TABLE service.game_localization (
                id BIGINT PRIMARY KEY,
                game_id BIGINT NOT NULL,
                region_id BIGINT NULL,
                name TEXT NULL
            )
            """.trimIndent(),
            "CREATE TABLE service.game_status (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.game_type (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.language (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.region (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.release_region (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.release_status (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.genre (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.theme (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.player_perspective (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.game_mode (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.keyword (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.company (id BIGINT PRIMARY KEY)",
            """
            CREATE TABLE service.game_language (
                game_id BIGINT NOT NULL,
                language_id BIGINT NOT NULL,
                supports_audio BOOLEAN NOT NULL DEFAULT FALSE,
                supports_subtitles BOOLEAN NOT NULL DEFAULT FALSE,
                supports_interface BOOLEAN NOT NULL DEFAULT FALSE
            )
            """.trimIndent(),
            "CREATE TABLE service.game_genre (game_id BIGINT NOT NULL, genre_id BIGINT NOT NULL)",
            "CREATE TABLE service.game_theme (game_id BIGINT NOT NULL, theme_id BIGINT NOT NULL)",
            """
            CREATE TABLE service.game_player_perspective (
                game_id BIGINT NOT NULL,
                player_perspective_id BIGINT NOT NULL
            )
            """.trimIndent(),
            "CREATE TABLE service.game_game_mode (game_id BIGINT NOT NULL, game_mode_id BIGINT NOT NULL)",
            "CREATE TABLE service.game_keyword (game_id BIGINT NOT NULL, keyword_id BIGINT NOT NULL)",
            """
            CREATE TABLE service.game_company (
                game_id BIGINT NOT NULL,
                company_id BIGINT NOT NULL,
                is_developer BOOLEAN NOT NULL DEFAULT FALSE,
                is_publisher BOOLEAN NOT NULL DEFAULT FALSE,
                is_porting BOOLEAN NOT NULL DEFAULT FALSE,
                is_supporting BOOLEAN NOT NULL DEFAULT FALSE
            )
            """.trimIndent(),
            """
            CREATE TABLE service.game_relation (
                game_id BIGINT NOT NULL,
                related_game_id BIGINT NOT NULL,
                relation_type TEXT NOT NULL
            )
            """.trimIndent(),
            "CREATE TABLE service.etl_cursor (table_name TEXT PRIMARY KEY, last_synced_at BIGINT NOT NULL, synced_at TIMESTAMP NOT NULL)",
        )
    }

    private fun executeAll(vararg statements: String) {
        statements.forEach(ingestJdbc::execute)
    }
}
