package com.projectgc.batch.persistence.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.OffsetDateTime
import java.util.UUID
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes

/**
 * IGDB 원천 스키마의 ingest.game 테이블을 매핑한 엔티티.
 *
 * 컬럼 정의는 Flyway 스크립트( {@code V1.0.0__init_source_db.sql} )를 기준으로 하며,
 * 배열 타입은 Hibernate 6의 ARRAY 매핑을 통해 PostgreSQL bigint[] 컬럼을 그대로 보존한다.
 */
@Suppress("LongParameterList")
@Entity
@Table(name = "game", schema = "ingest")
data class IngestGameEntity(
    @Id
    @Column(name = "id", nullable = false)
    val id: Long,

    @Column(name = "name", nullable = false)
    val name: String,

    @Column(name = "alternative_names", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val alternativeNameIds: List<Long>?,

    @Column(name = "game_localizations", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val localizationIds: List<Long>?,

    @Column(name = "slug", nullable = false)
    val slug: String,

    @Column(name = "first_release_date")
    val firstReleaseDateEpoch: Long?,

    @Column(name = "release_dates", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val releaseDateIds: List<Long>?,

    @Column(name = "platforms", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val platformIds: List<Long>?,

    @Column(name = "game_status")
    val statusId: Long?,

    @Column(name = "game_type")
    val typeId: Long?,

    @Column(name = "language_supports", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val languageSupportIds: List<Long>?,

    @Column(name = "summary")
    val summary: String?,

    @Column(name = "storyline")
    val storyline: String?,

    @Column(name = "genres", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val genreIds: List<Long>?,

    @Column(name = "themes", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val themeIds: List<Long>?,

    @Column(name = "player_perspectives", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val playerPerspectiveIds: List<Long>?,

    @Column(name = "game_modes", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val gameModeIds: List<Long>?,

    @Column(name = "keywords", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val keywordIds: List<Long>?,

    @Column(name = "involved_companies", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val involvedCompanyIds: List<Long>?,

    @Column(name = "parent_game")
    val parentGameId: Long?,

    @Column(name = "remakes", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val remakeIds: List<Long>?,

    @Column(name = "remasters", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val remasterIds: List<Long>?,

    @Column(name = "ports", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val portIds: List<Long>?,

    @Column(name = "standalone_expansions", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val standaloneExpansionIds: List<Long>?,

    @Column(name = "similar_games", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val similarGameIds: List<Long>?,

    @Column(name = "cover")
    val coverId: Long?,

    @Column(name = "artworks", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val artworkIds: List<Long>?,

    @Column(name = "screenshots", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val screenshotIds: List<Long>?,

    @Column(name = "videos", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val videoIds: List<Long>?,

    @Column(name = "websites", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val websiteIds: List<Long>?,

    @Column(name = "tags", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val tagIds: List<Long>?,

    @Column(name = "checksum")
    val checksum: UUID?,

    @Column(name = "updated_at")
    val updatedAtEpoch: Long?,

    @Column(name = "ingested_at", nullable = false)
    val ingestedAt: OffsetDateTime
)
