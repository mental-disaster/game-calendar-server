package com.projectgc.batch.service

import com.projectgc.batch.client.ParseError
import java.time.Instant
import java.util.UUID

class TableSyncStats(
    val syncId: UUID,
    val tableName: String,
    val startedAt: Instant = Instant.now(),
) {
    var fetched: Int = 0
    var upserted: Int = 0
    val parseErrors: MutableList<ParseError> = mutableListOf()
    var finishedAt: Instant? = null
}
