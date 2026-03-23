package com.projectgc.batch.models.entity.ingest

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import java.util.UUID

@Entity
@Table(schema = "ingest", name = "game")
class IngestGameEntity {
    @Id
    var id: Long = 0

    var name: String? = null
    var slug: String? = null
    var summary: String? = null
    var storyline: String? = null
    var firstReleaseDate: Long? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var releaseDates: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var platforms: Array<Long>? = null

    var gameStatus: Long? = null
    var gameType: Long? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var languageSupports: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var genres: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var themes: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var playerPerspectives: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var gameModes: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var keywords: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var involvedCompanies: Array<Long>? = null

    var parentGame: Long? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var remakes: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var remasters: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var ports: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var standaloneExpansions: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var similarGames: Array<Long>? = null

    var cover: Long? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var artworks: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var screenshots: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var videos: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var websites: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var alternativeNames: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var gameLocalizations: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var tags: Array<Long>? = null

    var checksum: UUID? = null
    var updatedAt: Long? = null
}
