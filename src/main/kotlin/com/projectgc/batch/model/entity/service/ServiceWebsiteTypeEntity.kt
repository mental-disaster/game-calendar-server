package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "website_type", schema = "service")
class ServiceWebsiteTypeEntity : ServiceEntity() {

    @Column(name = "type", nullable = false)
    var type: String = ""
}
