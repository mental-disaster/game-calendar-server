package com.projectgc.batch.repository.ingest

import com.projectgc.batch.models.entity.ingest.IngestGenreEntity
import com.projectgc.batch.models.entity.ingest.IngestThemeEntity
import com.projectgc.batch.models.entity.ingest.IngestPlayerPerspectiveEntity
import com.projectgc.batch.models.entity.ingest.IngestGameModeEntity
import com.projectgc.batch.models.entity.ingest.IngestKeywordEntity
import com.projectgc.batch.models.entity.ingest.IngestLanguageSupportTypeEntity
import com.projectgc.batch.models.entity.ingest.IngestPlatformTypeEntity
import com.projectgc.batch.models.entity.ingest.IngestGameStatusEntity
import com.projectgc.batch.models.entity.ingest.IngestGameTypeEntity
import com.projectgc.batch.models.entity.ingest.IngestWebsiteTypeEntity
import com.projectgc.batch.models.entity.ingest.IngestLanguageEntity
import com.projectgc.batch.models.entity.ingest.IngestRegionEntity
import com.projectgc.batch.models.entity.ingest.IngestReleaseDateRegionEntity
import com.projectgc.batch.models.entity.ingest.IngestReleaseDateStatusEntity
import com.projectgc.batch.models.entity.ingest.IngestPlatformLogoEntity
import org.springframework.data.jpa.repository.JpaRepository

interface IngestGenreRepository : JpaRepository<IngestGenreEntity, Long>
interface IngestThemeRepository : JpaRepository<IngestThemeEntity, Long>
interface IngestPlayerPerspectiveRepository : JpaRepository<IngestPlayerPerspectiveEntity, Long>
interface IngestGameModeRepository : JpaRepository<IngestGameModeEntity, Long>
interface IngestKeywordRepository : JpaRepository<IngestKeywordEntity, Long>
interface IngestLanguageSupportTypeRepository : JpaRepository<IngestLanguageSupportTypeEntity, Long>
interface IngestPlatformTypeRepository : JpaRepository<IngestPlatformTypeEntity, Long>
interface IngestGameStatusRepository : JpaRepository<IngestGameStatusEntity, Long>
interface IngestGameTypeRepository : JpaRepository<IngestGameTypeEntity, Long>
interface IngestWebsiteTypeRepository : JpaRepository<IngestWebsiteTypeEntity, Long>
interface IngestLanguageRepository : JpaRepository<IngestLanguageEntity, Long>
interface IngestRegionRepository : JpaRepository<IngestRegionEntity, Long>
interface IngestReleaseDateRegionRepository : JpaRepository<IngestReleaseDateRegionEntity, Long>
interface IngestReleaseDateStatusRepository : JpaRepository<IngestReleaseDateStatusEntity, Long>
interface IngestPlatformLogoRepository : JpaRepository<IngestPlatformLogoEntity, Long>
