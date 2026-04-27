package com.projectgc.orchestration.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor

@Configuration
class OrchestrationConfiguration {

    @Bean("serviceEtlTaskExecutor")
    fun serviceEtlTaskExecutor() = ThreadPoolTaskExecutor().apply {
        corePoolSize = 1
        maxPoolSize = 1
        queueCapacity = 0
        setThreadNamePrefix("service-etl-")
        initialize()
    }
}
