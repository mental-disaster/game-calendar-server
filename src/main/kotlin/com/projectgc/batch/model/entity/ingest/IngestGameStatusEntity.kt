package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.game_status 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "game_status", schema = "ingest")
class IngestGameStatusEntity : IngestEntity() {

    @Column(name = "status", nullable = false)
    var status: String = ""

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
