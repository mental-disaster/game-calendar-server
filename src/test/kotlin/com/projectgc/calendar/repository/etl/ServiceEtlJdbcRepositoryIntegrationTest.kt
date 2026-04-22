package com.projectgc.calendar.repository.etl

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertEquals

@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class ServiceEtlJdbcRepositoryIntegrationTest {

    companion object {
        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
    }

    private lateinit var jdbc: JdbcTemplate
    private lateinit var repository: ServiceEtlJdbcRepository

    @BeforeEach
    fun setUp() {
        val dataSource = DriverManagerDataSource(
            postgres.jdbcUrl,
            postgres.username,
            postgres.password,
        )
        jdbc = JdbcTemplate(dataSource)
        repository = ServiceEtlJdbcRepository(jdbc)
        resetSchemas()
    }

    @Test
    fun `findAffectedGameIdsFromGameRelationProjectionDiff includes existing source game when related game is newly materialized in same run`() {
        jdbc.update("INSERT INTO ingest.game (id, similar_games) VALUES (1, ARRAY[2]::BIGINT[])")
        jdbc.update("INSERT INTO ingest.game (id) VALUES (2)")
        jdbc.update("INSERT INTO service.game (id) VALUES (1)")

        assertEquals(
            linkedSetOf(1L),
            repository.findAffectedGameIdsFromGameRelationProjectionDiff(),
        )
    }

    @Test
    fun `rebuildGameDependentBridgeProjections inserts game relation for related game materialized in same run`() {
        jdbc.update("INSERT INTO ingest.game (id, similar_games) VALUES (1, ARRAY[2]::BIGINT[])")
        jdbc.update("INSERT INTO ingest.game (id) VALUES (2)")
        jdbc.update("INSERT INTO service.game (id) VALUES (1)")
        jdbc.update("INSERT INTO service.game (id) VALUES (2)")
        jdbc.update("INSERT INTO service.game_relation (game_id, related_game_id, relation_type) VALUES (1, 99, 'SIMILAR')")

        repository.rebuildGameDependentBridgeProjections(linkedSetOf(1L, 2L))

        val relationRows = jdbc.query(
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
                language_support_type BIGINT NULL
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
                supporting BOOLEAN NULL
            )
            """.trimIndent(),
            "CREATE TABLE service.game (id BIGINT PRIMARY KEY)",
            "CREATE TABLE service.language (id BIGINT PRIMARY KEY)",
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
        )
    }

    private fun executeAll(vararg statements: String) {
        statements.forEach(jdbc::execute)
    }
}
