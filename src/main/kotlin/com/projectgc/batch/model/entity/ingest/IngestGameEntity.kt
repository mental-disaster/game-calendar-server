package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes

/**
 * ingest.game 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "game", schema = "ingest")
class IngestGameEntity : IngestEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "alternative_names", columnDefinition = "bigint[]")
    var alternativeNameIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "game_localizations", columnDefinition = "bigint[]")
    var gameLocalizationIds: List<Long>? = null

    @Column(name = "slug", nullable = false)
    var slug: String = ""

    @Column(name = "first_release_date")
    var firstReleaseDate: Long? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "release_dates", columnDefinition = "bigint[]")
    var releaseDateIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "platforms", columnDefinition = "bigint[]")
    var platformIds: List<Long>? = null

    @Column(name = "game_status")
    var gameStatusId: Long? = null

    @Column(name = "game_type")
    var gameTypeId: Long? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "language_supports", columnDefinition = "bigint[]")
    var languageSupportIds: List<Long>? = null

    @Column(name = "summary")
    var summary: String? = null

    @Column(name = "storyline")
    var storyline: String? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "genres", columnDefinition = "bigint[]")
    var genreIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "themes", columnDefinition = "bigint[]")
    var themeIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "player_perspectives", columnDefinition = "bigint[]")
    var playerPerspectiveIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "game_modes", columnDefinition = "bigint[]")
    var gameModeIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "keywords", columnDefinition = "bigint[]")
    var keywordIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "involved_companies", columnDefinition = "bigint[]")
    var involvedCompanyIds: List<Long>? = null

    @Column(name = "parent_game")
    var parentGameId: Long? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "remakes", columnDefinition = "bigint[]")
    var remakeIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "remasters", columnDefinition = "bigint[]")
    var remasterIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "ports", columnDefinition = "bigint[]")
    var portIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "standalone_expansions", columnDefinition = "bigint[]")
    var standaloneExpansionIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "similar_games", columnDefinition = "bigint[]")
    var similarGameIds: List<Long>? = null

    @Column(name = "cover")
    var coverId: Long? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "artworks", columnDefinition = "bigint[]")
    var artworkIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "screenshots", columnDefinition = "bigint[]")
    var screenshotIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "videos", columnDefinition = "bigint[]")
    var videoIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "websites", columnDefinition = "bigint[]")
    var websiteIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "tags", columnDefinition = "bigint[]")
    var tagNumbers: List<Long>? = null

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
