package com.projectgc.batch.persistence.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.OffsetDateTime
import java.util.UUID

@Entity
@Table(name = "game_video", schema = "ingest")
data class IngestGameVideoEntity(
    @Id
    @Column(name = "id", nullable = false)
    val id: Long,

    @Column(name = "game", nullable = false)
    val gameId: Long,

    @Column(name = "name")
    val name: String?,

    @Column(name = "video_id", nullable = false)
    val videoId: String,

    @Column(name = "checksum")
    val checksum: UUID?,

    @Column(name = "ingested_at", nullable = false)
    val ingestedAt: OffsetDateTime
)
