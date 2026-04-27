package com.projectgc.batch.persistence.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.OffsetDateTime
import java.util.UUID

@Entity
@Table(name = "platform", schema = "ingest")
data class IngestPlatformEntity(
    @Id
    @Column(name = "id", nullable = false)
    val id: Long,

    @Column(name = "name", nullable = false)
    val name: String,

    @Column(name = "abbreviation")
    val abbreviation: String?,

    @Column(name = "alternative_name")
    val alternativeName: String?,

    @Column(name = "platform_logo")
    val platformLogoId: Long?,

    @Column(name = "platform_type")
    val platformTypeId: Long?,

    @Column(name = "checksum")
    val checksum: UUID?,

    @Column(name = "updated_at")
    val updatedAtEpoch: Long?,

    @Column(name = "ingested_at", nullable = false)
    val ingestedAt: OffsetDateTime
)
