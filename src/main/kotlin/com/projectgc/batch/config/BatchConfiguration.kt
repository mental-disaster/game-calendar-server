package com.projectgc.batch.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor

@Configuration
@EnableScheduling
@EnableAsync
class BatchConfiguration {

    @Bean("batchTaskExecutor")
    fun batchTaskExecutor() = ThreadPoolTaskExecutor().apply {
        corePoolSize = 1
        maxPoolSize = 1
        queueCapacity = 0   // 큐 없음 — 실행 중이면 즉시 TaskRejectedException 발생 (순차 적체 방지)
        setThreadNamePrefix("batch-")
        initialize()
    }
}
