package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes

/**
 * ingest.company 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "company", schema = "ingest")
class IngestCompanyEntity : IngestEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "parent")
    var parentCompanyId: Long? = null

    @Column(name = "changed_company_id")
    var changedCompanyId: Long? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "developed", columnDefinition = "bigint[]")
    var developedGameIds: List<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "published", columnDefinition = "bigint[]")
    var publishedGameIds: List<Long>? = null

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
