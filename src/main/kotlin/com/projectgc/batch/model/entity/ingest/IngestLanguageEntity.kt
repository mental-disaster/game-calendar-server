package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.language 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "language", schema = "ingest")
class IngestLanguageEntity : IngestEntity() {

    @Column(name = "locale", nullable = false)
    var locale: String = ""

    @Column(name = "name", nullable = false)
    var englishName: String = ""

    @Column(name = "native_name")
    var nativeName: String? = null

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
