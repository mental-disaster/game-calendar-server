package com.projectgc.batch.models.entity.ingest

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant

@Entity
@Table(schema = "ingest", name = "sync_cursor")
class IngestSyncCursorEntity {
    @Id
    @Column(name = "table_name")
    var tableName: String = ""

    var lastSyncedAt: Long = 0L

    var syncedAt: Instant = Instant.now()   // DB: synced_at TIMESTAMPTZ — 커서 마지막 갱신 시각
}
