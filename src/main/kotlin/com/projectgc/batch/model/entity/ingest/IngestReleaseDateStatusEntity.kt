package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.release_date_status 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "release_date_status", schema = "ingest")
class IngestReleaseDateStatusEntity : IngestEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "description")
    var description: String? = null

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
