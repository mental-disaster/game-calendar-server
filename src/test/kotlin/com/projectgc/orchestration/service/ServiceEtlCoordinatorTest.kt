package com.projectgc.orchestration.service

import com.projectgc.calendar.service.etl.ServiceEtlRunner
import com.projectgc.calendar.service.etl.ServiceEtlTrigger
import org.junit.jupiter.api.Test
import org.springframework.core.task.SyncTaskExecutor
import org.springframework.core.task.TaskRejectedException
import org.springframework.core.task.SimpleAsyncTaskExecutor
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ServiceEtlCoordinatorTest {

    @Test
    fun `rejects duplicate triggers while service etl is already running`() {
        val started = CountDownLatch(1)
        val released = CountDownLatch(1)
        val finished = CountDownLatch(1)
        val runner = object : ServiceEtlRunner {
            override fun run(runId: UUID, trigger: ServiceEtlTrigger) {
                started.countDown()
                released.await(5, TimeUnit.SECONDS)
                finished.countDown()
            }
        }
        val coordinator = ServiceEtlCoordinator(
            serviceEtlRunner = runner,
            serviceEtlTaskExecutor = SimpleAsyncTaskExecutor("service-etl-test-"),
        )

        coordinator.triggerAsync(ServiceEtlTrigger.manual())
        assertTrue(started.await(2, TimeUnit.SECONDS))

        assertFailsWith<TaskRejectedException> {
            coordinator.triggerAsync(ServiceEtlTrigger.manual())
        }

        released.countDown()
        assertTrue(finished.await(2, TimeUnit.SECONDS))
    }

    @Test
    fun `accepts a new trigger after previous run completes`() {
        val runner = RecordingRunner()
        val coordinator = ServiceEtlCoordinator(
            serviceEtlRunner = runner,
            serviceEtlTaskExecutor = SyncTaskExecutor(),
        )

        coordinator.triggerAsync(ServiceEtlTrigger.manual())
        coordinator.triggerAsync(ServiceEtlTrigger.manual())

        assertEquals(2, runner.invocationCount)
    }

    private class RecordingRunner : ServiceEtlRunner {
        var invocationCount: Int = 0

        override fun run(runId: UUID, trigger: ServiceEtlTrigger) {
            invocationCount += 1
        }
    }
}
