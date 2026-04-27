package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "game_mode", schema = "service")
class ServiceGameModeEntity : ServiceEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""
}
