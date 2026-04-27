package com.projectgc.batch.models.entity.ingest

import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import java.util.UUID

@Entity
@Table(schema = "ingest", name = "company")
class IngestCompanyEntity {
    @Id
    var id: Long = 0

    var name: String? = null
    var parent: Long? = null
    var changedCompanyId: Long? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var developed: Array<Long>? = null

    @JdbcTypeCode(SqlTypes.ARRAY)
    var published: Array<Long>? = null

    var checksum: UUID? = null
    var updatedAt: Long? = null
}
