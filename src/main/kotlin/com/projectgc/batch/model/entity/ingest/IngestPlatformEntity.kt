package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.platform 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "platform", schema = "ingest")
class IngestPlatformEntity : IngestEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "abbreviation")
    var abbreviation: String? = null

    @Column(name = "alternative_name")
    var alternativeName: String? = null

    @Column(name = "platform_logo")
    var platformLogoId: Long? = null

    @Column(name = "platform_type")
    var platformTypeId: Long? = null

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
