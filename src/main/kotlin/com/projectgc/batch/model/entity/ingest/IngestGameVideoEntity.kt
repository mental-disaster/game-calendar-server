package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.game_video 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "game_video", schema = "ingest")
class IngestGameVideoEntity : IngestEntity() {

    @Column(name = "game", nullable = false)
    var gameId: Long = 0L

    @Column(name = "name")
    var name: String? = null

    @Column(name = "video_id", nullable = false)
    var videoId: String = ""
}
