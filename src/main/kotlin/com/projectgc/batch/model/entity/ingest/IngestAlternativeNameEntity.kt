package com.projectgc.batch.model.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * ingest.alternative_name 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "alternative_name", schema = "ingest")
class IngestAlternativeNameEntity : IngestEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "comment")
    var comment: String? = null

    @Column(name = "game", nullable = false)
    var gameId: Long = 0L
}
