package com.projectgc.batch.persistence.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.OffsetDateTime
import java.util.UUID

@Entity
@Table(name = "website_type", schema = "ingest")
data class IngestWebsiteTypeEntity(
    @Id
    @Column(name = "id", nullable = false)
    val id: Long,

    @Column(name = "type", nullable = false)
    val type: String,

    @Column(name = "checksum")
    val checksum: UUID?,

    @Column(name = "updated_at")
    val updatedAtEpoch: Long?,

    @Column(name = "ingested_at", nullable = false)
    val ingestedAt: OffsetDateTime
)
