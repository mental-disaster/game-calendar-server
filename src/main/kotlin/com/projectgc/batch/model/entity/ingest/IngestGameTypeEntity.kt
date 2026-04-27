package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.game_type 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "game_type", schema = "ingest")
class IngestGameTypeEntity : IngestEntity() {

    @Column(name = "type", nullable = false)
    var type: String = ""

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
