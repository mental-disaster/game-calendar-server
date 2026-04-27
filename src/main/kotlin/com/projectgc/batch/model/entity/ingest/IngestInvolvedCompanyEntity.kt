package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.involved_company 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "involved_company", schema = "ingest")
class IngestInvolvedCompanyEntity : IngestEntity() {

    @Column(name = "game", nullable = false)
    var gameId: Long = 0L

    @Column(name = "company", nullable = false)
    var companyId: Long = 0L

    @Column(name = "developer")
    var developer: Boolean? = null

    @Column(name = "publisher")
    var publisher: Boolean? = null

    @Column(name = "porting")
    var porting: Boolean? = null

    @Column(name = "supporting")
    var supporting: Boolean? = null

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
