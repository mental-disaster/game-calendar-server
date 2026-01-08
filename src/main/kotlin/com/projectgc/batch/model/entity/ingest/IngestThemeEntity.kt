package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.theme 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "theme", schema = "ingest")
class IngestThemeEntity : IngestEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
