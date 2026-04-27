package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.language_support 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "language_support", schema = "ingest")
class IngestLanguageSupportEntity : IngestEntity() {

    @Column(name = "game", nullable = false)
    var gameId: Long = 0L

    @Column(name = "language", nullable = false)
    var languageId: Long = 0L

    @Column(name = "language_support_type")
    var languageSupportTypeId: Long? = null

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
