package com.projectgc.batch.models.entity.ingest

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.util.UUID

@Entity
@Table(schema = "ingest", name = "artwork")
class IngestArtworkEntity {
    @Id
    var id: Long = 0
    var game: Long = 0
    var imageId: String? = null
    var url: String? = null
    var checksum: UUID? = null
}

@Entity
@Table(schema = "ingest", name = "screenshot")
class IngestScreenshotEntity {
    @Id
    var id: Long = 0
    var game: Long = 0
    var imageId: String? = null
    var url: String? = null
    var checksum: UUID? = null
}

@Entity
@Table(schema = "ingest", name = "game_video")
class IngestGameVideoEntity {
    @Id
    var id: Long = 0
    var game: Long = 0
    var name: String? = null
    var videoId: String? = null
    var checksum: UUID? = null
}

@Entity
@Table(schema = "ingest", name = "website")
class IngestWebsiteEntity {
    @Id
    var id: Long = 0
    var game: Long = 0
    var type: Long? = null
    var url: String? = null
    var trusted: Boolean? = null
    var checksum: UUID? = null
}

@Entity
@Table(schema = "ingest", name = "alternative_name")
class IngestAlternativeNameEntity {
    @Id
    var id: Long = 0
    var game: Long = 0
    var name: String? = null
    var comment: String? = null
    var checksum: UUID? = null
}
