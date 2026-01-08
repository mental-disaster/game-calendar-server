package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "release_status", schema = "service")
class ServiceReleaseStatusEntity : ServiceEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "description")
    var description: String? = null
}
