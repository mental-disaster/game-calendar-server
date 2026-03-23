package com.projectgc.batch.models.entity.ingest

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.util.UUID

@Entity
@Table(schema = "ingest", name = "language_support")
class IngestLanguageSupportEntity {
    @Id
    var id: Long = 0

    var game: Long = 0
    var language: Long = 0
    var languageSupportType: Long? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}
