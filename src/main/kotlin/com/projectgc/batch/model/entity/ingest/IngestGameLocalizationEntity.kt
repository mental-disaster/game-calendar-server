package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.game_localization 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "game_localization", schema = "ingest")
class IngestGameLocalizationEntity : IngestEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "game", nullable = false)
    var gameId: Long = 0L

    @Column(name = "region")
    var regionId: Long? = null

    @Column(name = "cover")
    var coverId: Long? = null

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
