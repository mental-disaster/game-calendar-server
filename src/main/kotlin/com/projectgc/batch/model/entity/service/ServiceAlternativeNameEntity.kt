package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * service.alternative_name 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "alternative_name", schema = "service")
class ServiceAlternativeNameEntity : ServiceEntity() {

    @Column(name = "game_id", nullable = false)
    var gameId: Long = 0L

    @Column(name = "name", nullable = false)
    var alternativeName: String = ""

    @Column(name = "comment")
    var comment: String? = null
}
