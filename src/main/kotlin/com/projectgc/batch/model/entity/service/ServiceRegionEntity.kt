package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "region", schema = "service")
class ServiceRegionEntity : ServiceEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "identifier")
    var identifier: String? = null
}
