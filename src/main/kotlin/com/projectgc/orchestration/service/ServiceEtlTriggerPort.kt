package com.projectgc.orchestration.service

import com.projectgc.calendar.service.etl.ServiceEtlTrigger
import java.util.UUID

interface ServiceEtlTriggerPort {
    fun triggerAsync(trigger: ServiceEtlTrigger): UUID
}
