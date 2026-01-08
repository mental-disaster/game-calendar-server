package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "artwork", schema = "service")
class ServiceArtworkEntity : ServiceEntity() {

    @Column(name = "game_id", nullable = false)
    var gameId: Long = 0L

    @Column(name = "image_id", nullable = false)
    var imageId: String = ""

    @Column(name = "url")
    var url: String? = null
}
