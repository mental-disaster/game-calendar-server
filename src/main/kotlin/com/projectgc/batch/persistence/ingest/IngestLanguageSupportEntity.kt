package com.projectgc.batch.persistence.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.OffsetDateTime
import java.util.UUID

@Entity
@Table(name = "language_support", schema = "ingest")
data class IngestLanguageSupportEntity(
    @Id
    @Column(name = "id", nullable = false)
    val id: Long,

    @Column(name = "game", nullable = false)
    val gameId: Long,

    @Column(name = "language", nullable = false)
    val languageId: Long,

    @Column(name = "language_support_type")
    val languageSupportTypeId: Long?,

    @Column(name = "checksum")
    val checksum: UUID?,

    @Column(name = "updated_at")
    val updatedAtEpoch: Long?,

    @Column(name = "ingested_at", nullable = false)
    val ingestedAt: OffsetDateTime
)
