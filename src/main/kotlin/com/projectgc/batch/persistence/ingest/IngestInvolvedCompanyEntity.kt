package com.projectgc.batch.persistence.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.OffsetDateTime
import java.util.UUID

@Entity
@Table(name = "involved_company", schema = "ingest")
data class IngestInvolvedCompanyEntity(
    @Id
    @Column(name = "id", nullable = false)
    val id: Long,

    @Column(name = "game", nullable = false)
    val gameId: Long,

    @Column(name = "company", nullable = false)
    val companyId: Long,

    @Column(name = "developer")
    val developer: Boolean?,

    @Column(name = "publisher")
    val publisher: Boolean?,

    @Column(name = "porting")
    val porting: Boolean?,

    @Column(name = "supporting")
    val supporting: Boolean?,

    @Column(name = "checksum")
    val checksum: UUID?,

    @Column(name = "updated_at")
    val updatedAtEpoch: Long?,

    @Column(name = "ingested_at", nullable = false)
    val ingestedAt: OffsetDateTime
)
