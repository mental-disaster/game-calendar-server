package com.projectgc.batch.repository.ingest

import com.projectgc.batch.models.entity.ingest.IngestLanguageSupportEntity
import org.springframework.data.jpa.repository.JpaRepository

interface IngestLanguageSupportRepository : JpaRepository<IngestLanguageSupportEntity, Long>
