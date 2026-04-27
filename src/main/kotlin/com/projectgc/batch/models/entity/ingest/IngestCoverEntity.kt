package com.projectgc.batch.models.entity.ingest

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.util.UUID

@Entity
@Table(schema = "ingest", name = "cover")
class IngestCoverEntity {
    @Id
    var id: Long = 0

    var game: Long? = null
    var gameLocalization: Long? = null
    var imageId: String? = null
    var url: String? = null
    var checksum: UUID? = null
}
