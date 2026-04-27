package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "game_type", schema = "service")
class ServiceGameTypeEntity : ServiceEntity() {

    @Column(name = "type", nullable = false)
    var type: String = ""
}
