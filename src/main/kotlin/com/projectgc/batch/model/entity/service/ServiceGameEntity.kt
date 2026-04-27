package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table
import java.time.Instant

/**
 * service.game 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "game", schema = "service")
class ServiceGameEntity : ServiceEntity() {

    @Column(name = "slug", nullable = false, unique = true)
    var slug: String = ""

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "summary")
    var summary: String? = null

    @Column(name = "storyline")
    var storyline: String? = null

    @Column(name = "first_release_date")
    var firstReleaseDate: Instant? = null

    @Column(name = "status_id")
    var statusId: Long? = null

    @Column(name = "type_id")
    var typeId: Long? = null

    @Column(name = "source_updated_at")
    var sourceUpdatedAt: Instant? = null

    @Column(name = "created_at", nullable = false, insertable = false, updatable = false)
    var createdAt: Instant? = null

    @Column(name = "updated_at", nullable = false, insertable = false)
    var updatedAt: Instant? = null

    @Column(name = "search_vector", columnDefinition = "tsvector", insertable = false, updatable = false)
    var searchVector: String? = null
}
