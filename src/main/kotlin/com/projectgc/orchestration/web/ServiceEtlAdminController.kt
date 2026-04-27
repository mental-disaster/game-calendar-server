package com.projectgc.orchestration.web

import com.projectgc.calendar.service.etl.ServiceEtlTrigger
import com.projectgc.orchestration.service.ServiceEtlTriggerPort
import org.slf4j.LoggerFactory
import org.springframework.core.task.TaskRejectedException
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/batch")
class ServiceEtlAdminController(
    private val serviceEtlTriggerPort: ServiceEtlTriggerPort,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @PostMapping("/service-sync")
    fun triggerServiceSync(): ResponseEntity<String> {
        return try {
            val runId = serviceEtlTriggerPort.triggerAsync(ServiceEtlTrigger.manual())
            log.info("수동 service ETL 요청 수신 (runId=$runId)")
            ResponseEntity.accepted().body("service ETL이 백그라운드에서 시작되었습니다. runId=$runId")
        } catch (ex: TaskRejectedException) {
            log.warn("service ETL 이미 실행 중 - 중복 요청 거부")
            ResponseEntity.status(409).body("service ETL이 이미 실행 중입니다.")
        }
    }
}
