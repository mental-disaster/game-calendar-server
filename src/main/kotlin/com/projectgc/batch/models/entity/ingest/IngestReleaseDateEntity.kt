package com.projectgc.batch.models.entity.ingest

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.util.UUID

@Entity
@Table(schema = "ingest", name = "release_date")
class IngestReleaseDateEntity {
    @Id
    var id: Long = 0

    var game: Long = 0
    var platform: Long? = null
    var releaseRegion: Long? = null
    var status: Long? = null
    var date: Long? = null
    var y: Int? = null
    var m: Int? = null
    var human: String? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}
