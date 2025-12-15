package com.projectgc.batch.persistence.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.OffsetDateTime
import java.util.UUID
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes

@Suppress("LongParameterList")
@Entity
@Table(name = "company", schema = "ingest")
data class IngestCompanyEntity(
    @Id
    @Column(name = "id", nullable = false)
    val id: Long,

    @Column(name = "name", nullable = false)
    val name: String,

    @Column(name = "parent")
    val parentCompanyId: Long?,

    @Column(name = "changed_company_id")
    val changedCompanyId: Long?,

    @Column(name = "developed", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val developedGameIds: List<Long>?,

    @Column(name = "published", columnDefinition = "bigint[]")
    @JdbcTypeCode(SqlTypes.ARRAY)
    val publishedGameIds: List<Long>?,

    @Column(name = "checksum")
    val checksum: UUID?,

    @Column(name = "updated_at")
    val updatedAtEpoch: Long?,

    @Column(name = "ingested_at", nullable = false)
    val ingestedAt: OffsetDateTime
)
