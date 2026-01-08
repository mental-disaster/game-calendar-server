package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.release_date 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "release_date", schema = "ingest")
class IngestReleaseDateEntity : IngestEntity() {

    @Column(name = "game", nullable = false)
    var gameId: Long = 0L

    @Column(name = "platform")
    var platformId: Long? = null

    @Column(name = "release_region")
    var releaseRegionId: Long? = null

    @Column(name = "status")
    var statusId: Long? = null

    @Column(name = "date")
    var releaseTimestamp: Long? = null

    @Column(name = "y")
    var releaseYear: Int? = null

    @Column(name = "m")
    var releaseMonth: Int? = null

    @Column(name = "human")
    var humanReadableDate: String? = null

    @Column(name = "updated_at")
    var updatedAt: Long? = null
}
