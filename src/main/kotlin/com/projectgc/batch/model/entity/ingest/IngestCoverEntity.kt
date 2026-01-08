package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.cover 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "cover", schema = "ingest")
class IngestCoverEntity : IngestEntity() {

    @Column(name = "game")
    var gameId: Long? = null

    @Column(name = "game_localization")
    var gameLocalizationId: Long? = null

    @Column(name = "image_id", nullable = false)
    var imageId: String = ""

    @Column(name = "url")
    var url: String? = null
}
