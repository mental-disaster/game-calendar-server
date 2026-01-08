package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "language", schema = "service")
class ServiceLanguageEntity : ServiceEntity() {

    @Column(name = "locale", nullable = false)
    var locale: String = ""

    @Column(name = "name", nullable = false)
    var englishName: String = ""

    @Column(name = "native_name")
    var nativeName: String? = null
}
