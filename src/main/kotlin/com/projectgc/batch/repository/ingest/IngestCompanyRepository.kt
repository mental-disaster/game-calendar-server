package com.projectgc.batch.repository.ingest

import com.projectgc.batch.models.entity.ingest.IngestCompanyEntity
import org.springframework.data.jpa.repository.JpaRepository

interface IngestCompanyRepository : JpaRepository<IngestCompanyEntity, Long>
