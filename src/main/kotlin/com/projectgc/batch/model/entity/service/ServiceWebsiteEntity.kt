package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "website", schema = "service")
class ServiceWebsiteEntity : ServiceEntity() {

    @Column(name = "game_id", nullable = false)
    var gameId: Long = 0L

    @Column(name = "type_id")
    var typeId: Long? = null

    @Column(name = "url", nullable = false)
    var url: String = ""

    @Column(name = "is_trusted", nullable = false)
    var trusted: Boolean = false
}
