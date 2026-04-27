package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "game_status", schema = "service")
class ServiceGameStatusEntity : ServiceEntity() {

    @Column(name = "status", nullable = false)
    var status: String = ""
}
