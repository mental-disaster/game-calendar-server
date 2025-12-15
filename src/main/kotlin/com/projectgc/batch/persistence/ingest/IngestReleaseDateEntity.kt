package com.projectgc.batch.persistence.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.OffsetDateTime
import java.util.UUID

@Suppress("LongParameterList")
@Entity
@Table(name = "release_date", schema = "ingest")
data class IngestReleaseDateEntity(
    @Id
    @Column(name = "id", nullable = false)
    val id: Long,

    @Column(name = "game", nullable = false)
    val gameId: Long,

    @Column(name = "platform")
    val platformId: Long?,

    @Column(name = "release_region")
    val releaseRegionId: Long?,

    @Column(name = "status")
    val statusId: Long?,

    @Column(name = "date")
    val releaseDateEpoch: Long?,

    @Column(name = "y")
    val year: Int?,

    @Column(name = "m")
    val month: Int?,

    @Column(name = "human")
    val humanReadableDate: String?,

    @Column(name = "checksum")
    val checksum: UUID?,

    @Column(name = "updated_at")
    val updatedAtEpoch: Long?,

    @Column(name = "ingested_at", nullable = false)
    val ingestedAt: OffsetDateTime
)
