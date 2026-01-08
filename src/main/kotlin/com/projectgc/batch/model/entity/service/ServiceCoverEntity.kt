package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "cover", schema = "service")
class ServiceCoverEntity : ServiceEntity() {

    @Column(name = "game_id", nullable = false)
    var gameId: Long = 0L

    @Column(name = "game_localization_id")
    var gameLocalizationId: Long? = null

    @Column(name = "image_id", nullable = false)
    var imageId: String = ""

    @Column(name = "url")
    var url: String? = null

    @Column(name = "is_main", nullable = false)
    var main: Boolean = false
}
