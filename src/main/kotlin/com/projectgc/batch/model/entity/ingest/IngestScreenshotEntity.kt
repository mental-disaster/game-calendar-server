package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.screenshot 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "screenshot", schema = "ingest")
class IngestScreenshotEntity : IngestEntity() {

    @Column(name = "game", nullable = false)
    var gameId: Long = 0L

    @Column(name = "image_id", nullable = false)
    var imageId: String = ""

    @Column(name = "url")
    var url: String? = null
}
