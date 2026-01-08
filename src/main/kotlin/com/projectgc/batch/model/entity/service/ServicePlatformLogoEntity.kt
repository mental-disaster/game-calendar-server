package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "platform_logo", schema = "service")
class ServicePlatformLogoEntity : ServiceEntity() {

    @Column(name = "image_id", nullable = false)
    var imageId: String = ""

    @Column(name = "url")
    var url: String? = null
}
