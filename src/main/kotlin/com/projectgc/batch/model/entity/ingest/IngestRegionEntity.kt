package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.region 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "region", schema = "ingest")
class IngestRegionEntity : IngestEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "identifier")
    var identifier: String? = null

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
