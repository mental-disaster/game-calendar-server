package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.release_date_region 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "release_date_region", schema = "ingest")
class IngestReleaseDateRegionEntity : IngestEntity() {

    @Column(name = "region")
    var regionName: String? = null

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
