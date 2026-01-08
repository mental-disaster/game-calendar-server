package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

@Entity
@Table(name = "company", schema = "service")
class ServiceCompanyEntity : ServiceEntity() {

    @Column(name = "name", nullable = false)
    var name: String = ""

    @Column(name = "parent_company_id")
    var parentCompanyId: Long? = null

    @Column(name = "merged_into_company_id")
    var mergedIntoCompanyId: Long? = null
}
