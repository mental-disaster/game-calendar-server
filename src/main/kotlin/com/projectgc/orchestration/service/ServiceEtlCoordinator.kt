package com.projectgc.orchestration.service

import com.projectgc.calendar.service.etl.ServiceEtlRunner
import com.projectgc.calendar.service.etl.ServiceEtlTrigger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.core.task.TaskExecutor
import org.springframework.core.task.TaskRejectedException
import org.springframework.stereotype.Service
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

@Service
class ServiceEtlCoordinator(
    private val serviceEtlRunner: ServiceEtlRunner,
    @Qualifier("serviceEtlTaskExecutor")
    private val serviceEtlTaskExecutor: TaskExecutor,
) : ServiceEtlTriggerPort {
    private val log = LoggerFactory.getLogger(javaClass)
    private val running = AtomicBoolean(false)

    override fun triggerAsync(trigger: ServiceEtlTrigger): UUID {
        if (!running.compareAndSet(false, true)) {
            throw TaskRejectedException("service ETL is already running")
        }

        val runId = UUID.randomUUID()
        try {
            serviceEtlTaskExecutor.execute {
                try {
                    serviceEtlRunner.run(runId, trigger)
                } catch (ex: Exception) {
                    log.error("service ETL 비동기 실행 실패 (runId=$runId): ${ex.message}", ex)
                } finally {
                    running.set(false)
                }
            }
        } catch (ex: Exception) {
            running.set(false)
            if (ex is TaskRejectedException) {
                throw ex
            }
            throw TaskRejectedException("service ETL task submission failed", ex)
        }

        return runId
    }
}
