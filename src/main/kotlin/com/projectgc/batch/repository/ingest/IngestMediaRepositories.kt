package com.projectgc.batch.repository.ingest

import com.projectgc.batch.models.entity.ingest.IngestArtworkEntity
import com.projectgc.batch.models.entity.ingest.IngestScreenshotEntity
import com.projectgc.batch.models.entity.ingest.IngestGameVideoEntity
import com.projectgc.batch.models.entity.ingest.IngestWebsiteEntity
import com.projectgc.batch.models.entity.ingest.IngestAlternativeNameEntity
import org.springframework.data.jpa.repository.JpaRepository

interface IngestArtworkRepository : JpaRepository<IngestArtworkEntity, Long>
interface IngestScreenshotRepository : JpaRepository<IngestScreenshotEntity, Long>
interface IngestGameVideoRepository : JpaRepository<IngestGameVideoEntity, Long>
interface IngestWebsiteRepository : JpaRepository<IngestWebsiteEntity, Long>
interface IngestAlternativeNameRepository : JpaRepository<IngestAlternativeNameEntity, Long>
