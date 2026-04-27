package com.projectgc.shared.event

import java.time.Instant
import java.util.UUID

data class IngestSyncSucceededEvent(
    val syncId: UUID,
    val completedAt: Instant,
)
