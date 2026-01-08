package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.keyword 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "keyword", schema = "ingest")
class IngestKeywordEntity : IngestEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
