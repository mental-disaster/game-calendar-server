package com.projectgc.batch.models.entity.ingest

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.util.UUID

@Entity
@Table(schema = "ingest", name = "game_localization")
class IngestGameLocalizationEntity {
    @Id
    var id: Long = 0

    var game: Long = 0
    var region: Long? = null
    var name: String? = null
    var cover: Long? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}
