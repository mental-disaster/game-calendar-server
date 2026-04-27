package com.projectgc.batch.repository.ingest

import com.projectgc.batch.models.entity.ingest.IngestReleaseDateEntity
import org.springframework.data.jpa.repository.JpaRepository

interface IngestReleaseDateRepository : JpaRepository<IngestReleaseDateEntity, Long>
