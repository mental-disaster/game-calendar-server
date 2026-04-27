package com.projectgc.batch.models.entity.ingest

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.util.UUID

@Entity
@Table(schema = "ingest", name = "platform")
class IngestPlatformEntity {
    @Id
    var id: Long = 0

    var name: String? = null
    var abbreviation: String? = null
    var alternativeName: String? = null
    var platformLogo: Long? = null
    var platformType: Long? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}
