package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "platform_type", schema = "service")
class ServicePlatformTypeEntity : ServiceEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""
}
