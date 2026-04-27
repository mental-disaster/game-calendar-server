package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "genre", schema = "service")
class ServiceGenreEntity : ServiceEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""
}
