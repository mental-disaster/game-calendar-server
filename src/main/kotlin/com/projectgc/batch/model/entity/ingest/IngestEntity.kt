package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Id
import jakarta.persistence.MappedSuperclass
import java.time.Instant
import java.util.UUID

/**
 * ingest 스키마 공통 컬럼을 묶은 상위 엔티티입니다.
 */
@MappedSuperclass
abstract class IngestEntity(
    @Id
    @Column(name = "id")
    open var id: Long = 0L,

    @Column(name = "checksum")
    open var checksum: UUID? = null,

    @Column(name = "ingested_at", nullable = false, insertable = false, updatable = false)
    open var ingestedAt: Instant? = null
)
