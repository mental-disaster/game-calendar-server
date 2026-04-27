package com.projectgc.orchestration.event

import com.projectgc.calendar.service.etl.ServiceEtlTrigger
import com.projectgc.orchestration.service.ServiceEtlTriggerPort
import com.projectgc.shared.event.IngestSyncSucceededEvent
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.springframework.core.task.TaskRejectedException
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals

class IngestToServiceEtlOrchestratorTest {

    @Test
    fun `forwards authoritative ingest success to service etl coordinator`() {
        val triggerPort = RecordingTriggerPort()
        val orchestrator = IngestToServiceEtlOrchestrator(triggerPort)
        val syncId = UUID.randomUUID()

        orchestrator.handleIngestSyncSucceeded(
            IngestSyncSucceededEvent(
                syncId = syncId,
                completedAt = Instant.now(),
            )
        )

        assertEquals(listOf(ServiceEtlTrigger.afterIngest(syncId)), triggerPort.receivedTriggers)
    }

    @Test
    fun `swallows duplicate-run conflicts from service etl coordinator`() {
        val triggerPort = RecordingTriggerPort(
            failure = TaskRejectedException("service ETL is already running"),
        )
        val orchestrator = IngestToServiceEtlOrchestrator(triggerPort)

        assertDoesNotThrow {
            orchestrator.handleIngestSyncSucceeded(
                IngestSyncSucceededEvent(
                    syncId = UUID.randomUUID(),
                    completedAt = Instant.now(),
                )
            )
        }
    }

    private class RecordingTriggerPort(
        private val failure: Exception? = null,
    ) : ServiceEtlTriggerPort {
        val receivedTriggers = mutableListOf<ServiceEtlTrigger>()

        override fun triggerAsync(trigger: ServiceEtlTrigger) = failure?.let { throw it } ?: UUID.randomUUID().also {
            receivedTriggers += trigger
        }
    }
}
