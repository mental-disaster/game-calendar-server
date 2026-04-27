package com.projectgc.orchestration.event

import com.projectgc.calendar.service.etl.ServiceEtlTrigger
import com.projectgc.orchestration.service.ServiceEtlTriggerPort
import com.projectgc.shared.event.IngestSyncSucceededEvent
import org.slf4j.LoggerFactory
import org.springframework.context.event.EventListener
import org.springframework.core.task.TaskRejectedException
import org.springframework.stereotype.Component

@Component
class IngestToServiceEtlOrchestrator(
    private val serviceEtlTriggerPort: ServiceEtlTriggerPort,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @EventListener
    fun handleIngestSyncSucceeded(event: IngestSyncSucceededEvent) {
        runCatching {
            val runId = serviceEtlTriggerPort.triggerAsync(ServiceEtlTrigger.afterIngest(event.syncId))
            log.info("ingest 완료 후 service ETL 자동 실행 요청 수락 (ingestSyncId=${event.syncId}, serviceRunId=$runId)")
        }.onFailure { ex ->
            when (ex) {
                is TaskRejectedException -> log.warn(
                    "service ETL 자동 실행 스킵 - 이미 실행 중입니다. (ingestSyncId=${event.syncId}, completedAt=${event.completedAt})"
                )
                else -> log.error(
                    "service ETL 자동 실행 요청 실패 (ingestSyncId=${event.syncId}): ${ex.message}",
                    ex
                )
            }
        }
    }
}
