package com.projectgc.batch.repository.ingest

import com.projectgc.batch.models.entity.ingest.IngestSyncCursorEntity
import org.springframework.data.jpa.repository.JpaRepository

interface IngestSyncCursorRepository : JpaRepository<IngestSyncCursorEntity, String>
