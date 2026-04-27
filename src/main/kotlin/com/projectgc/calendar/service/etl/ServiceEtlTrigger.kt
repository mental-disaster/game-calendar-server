package com.projectgc.calendar.service.etl

import java.util.UUID

enum class ServiceEtlTriggerType {
    MANUAL,
    INGEST_SYNC_COMPLETED,
}

data class ServiceEtlTrigger(
    val type: ServiceEtlTriggerType,
    val ingestSyncId: UUID? = null,
) {
    companion object {
        fun manual() = ServiceEtlTrigger(type = ServiceEtlTriggerType.MANUAL)

        fun afterIngest(syncId: UUID) = ServiceEtlTrigger(
            type = ServiceEtlTriggerType.INGEST_SYNC_COMPLETED,
            ingestSyncId = syncId,
        )
    }
}
