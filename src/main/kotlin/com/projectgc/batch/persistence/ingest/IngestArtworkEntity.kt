package com.projectgc.batch.persistence.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.OffsetDateTime
import java.util.UUID

@Entity
@Table(name = "artwork", schema = "ingest")
data class IngestArtworkEntity(
    @Id
    @Column(name = "id", nullable = false)
    val id: Long,

    @Column(name = "game", nullable = false)
    val gameId: Long,

    @Column(name = "image_id", nullable = false)
    val imageId: String,

    @Column(name = "url")
    val url: String?,

    @Column(name = "checksum")
    val checksum: UUID?,

    @Column(name = "ingested_at", nullable = false)
    val ingestedAt: OffsetDateTime
)
