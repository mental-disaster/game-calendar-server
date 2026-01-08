package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "platform", schema = "service")
class ServicePlatformEntity : ServiceEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "abbreviation")
    var abbreviation: String? = null

    @Column(name = "alternative_name")
    var alternativeName: String? = null

    @Column(name = "logo_id")
    var logoId: Long? = null

    @Column(name = "type_id")
    var typeId: Long? = null
}
