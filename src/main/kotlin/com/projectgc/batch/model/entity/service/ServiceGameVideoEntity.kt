package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "game_video", schema = "service")
class ServiceGameVideoEntity : ServiceEntity() {

    @Column(name = "game_id", nullable = false)
    var gameId: Long = 0L

    @Column(name = "name")
    var name: String? = null

    @Column(name = "video_id", nullable = false)
    var videoId: String = ""
}
