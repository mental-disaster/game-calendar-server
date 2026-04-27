package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.website 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "website", schema = "ingest")
class IngestWebsiteEntity : IngestEntity() {

    @Column(name = "game", nullable = false)
    var gameId: Long = 0L

    @Column(name = "type")
    var typeId: Long? = null

    @Column(name = "url", nullable = false)
    var url: String = ""

    @Column(name = "trusted")
    var trusted: Boolean? = null
}
