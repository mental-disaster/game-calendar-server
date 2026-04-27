package com.projectgc.batch.models.entity.ingest

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.util.UUID

@Entity
@Table(schema = "ingest", name = "genre")
class IngestGenreEntity {
    @Id
    var id: Long = 0
    var name: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "theme")
class IngestThemeEntity {
    @Id
    var id: Long = 0
    var name: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "player_perspective")
class IngestPlayerPerspectiveEntity {
    @Id
    var id: Long = 0
    var name: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "game_mode")
class IngestGameModeEntity {
    @Id
    var id: Long = 0
    var name: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "keyword")
class IngestKeywordEntity {
    @Id
    var id: Long = 0
    var name: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "language_support_type")
class IngestLanguageSupportTypeEntity {
    @Id
    var id: Long = 0
    var name: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "platform_type")
class IngestPlatformTypeEntity {
    @Id
    var id: Long = 0
    var name: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "game_status")
class IngestGameStatusEntity {
    @Id
    var id: Long = 0
    var status: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "game_type")
class IngestGameTypeEntity {
    @Id
    var id: Long = 0
    var type: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "website_type")
class IngestWebsiteTypeEntity {
    @Id
    var id: Long = 0
    var type: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "language")
class IngestLanguageEntity {
    @Id
    var id: Long = 0
    var locale: String? = null
    var name: String? = null
    var nativeName: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "region")
class IngestRegionEntity {
    @Id
    var id: Long = 0
    var name: String? = null
    var identifier: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "release_date_region")
class IngestReleaseDateRegionEntity {
    @Id
    var id: Long = 0
    var region: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "release_date_status")
class IngestReleaseDateStatusEntity {
    @Id
    var id: Long = 0
    var name: String? = null
    var description: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}

@Entity
@Table(schema = "ingest", name = "platform_logo")
class IngestPlatformLogoEntity {
    @Id
    var id: Long = 0
    var imageId: String? = null
    var url: String? = null
    var checksum: UUID? = null
}
