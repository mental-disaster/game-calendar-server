package com.projectgc

import com.projectgc.batch.config.IgdbProperties
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication

@SpringBootApplication
@EnableConfigurationProperties(IgdbProperties::class)
class ProjectgcApplication

fun main(args: Array<String>) {
	runApplication<ProjectgcApplication>(*args)
}
