package com.projectgc.batch.persistence.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.OffsetDateTime
import java.util.UUID

@Entity
@Table(name = "website", schema = "ingest")
data class IngestWebsiteEntity(
    @Id
    @Column(name = "id", nullable = false)
    val id: Long,

    @Column(name = "game", nullable = false)
    val gameId: Long,

    @Column(name = "type")
    val typeId: Long?,

    @Column(name = "url", nullable = false)
    val url: String,

    @Column(name = "trusted")
    val trusted: Boolean?,

    @Column(name = "checksum")
    val checksum: UUID?,

    @Column(name = "ingested_at", nullable = false)
    val ingestedAt: OffsetDateTime
)
