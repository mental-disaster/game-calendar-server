package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.platform_logo 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "platform_logo", schema = "ingest")
class IngestPlatformLogoEntity : IngestEntity() {

    @Column(name = "image_id", nullable = false)
    var imageId: String = ""

    @Column(name = "url")
    var url: String? = null
}
