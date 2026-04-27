package com.projectgc.batch.models.entity.ingest

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.util.UUID

@Entity
@Table(schema = "ingest", name = "involved_company")
class IngestInvolvedCompanyEntity {
    @Id
    var id: Long = 0

    var game: Long = 0
    var company: Long = 0
    var developer: Boolean? = null
    var publisher: Boolean? = null
    var porting: Boolean? = null
    var supporting: Boolean? = null
    var checksum: UUID? = null
    var updatedAt: Long? = null
}
