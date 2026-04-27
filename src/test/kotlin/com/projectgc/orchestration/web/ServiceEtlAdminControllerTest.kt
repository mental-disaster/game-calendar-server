package com.projectgc.orchestration.web

import com.projectgc.calendar.service.etl.ServiceEtlTrigger
import com.projectgc.orchestration.service.ServiceEtlTriggerPort
import org.junit.jupiter.api.Test
import org.springframework.core.task.TaskRejectedException
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ServiceEtlAdminControllerTest {

    @Test
    fun `returns accepted when service etl starts asynchronously`() {
        val triggerPort = RecordingTriggerPort()
        val controller = ServiceEtlAdminController(triggerPort)

        val response = controller.triggerServiceSync()

        assertEquals(202, response.statusCode.value())
        assertEquals(listOf(ServiceEtlTrigger.manual()), triggerPort.receivedTriggers)
        assertTrue(response.body.orEmpty().contains("runId="))
    }

    @Test
    fun `returns conflict when service etl is already running`() {
        val triggerPort = RecordingTriggerPort(
            failure = TaskRejectedException("service ETL is already running"),
        )
        val controller = ServiceEtlAdminController(triggerPort)

        val response = controller.triggerServiceSync()

        assertEquals(409, response.statusCode.value())
        assertTrue(triggerPort.receivedTriggers.isEmpty())
    }

    private class RecordingTriggerPort(
        private val failure: Exception? = null,
    ) : ServiceEtlTriggerPort {
        val receivedTriggers = mutableListOf<ServiceEtlTrigger>()

        override fun triggerAsync(trigger: ServiceEtlTrigger) = failure?.let { throw it } ?: java.util.UUID.randomUUID().also {
            receivedTriggers += trigger
        }
    }
}
