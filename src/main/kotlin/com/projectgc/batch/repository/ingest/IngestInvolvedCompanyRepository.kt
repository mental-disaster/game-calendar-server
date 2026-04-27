package com.projectgc.batch.repository.ingest

import com.projectgc.batch.models.entity.ingest.IngestInvolvedCompanyEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query

interface IngestInvolvedCompanyRepository : JpaRepository<IngestInvolvedCompanyEntity, Long> {

    @Query("SELECT DISTINCT ic.company FROM IngestInvolvedCompanyEntity ic")
    fun findAllDistinctCompanyIds(): List<Long>
}
