package com.projectgc.calendar.config

import com.zaxxer.hikari.HikariDataSource
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import kotlin.test.assertEquals
import kotlin.test.assertNotSame
import kotlin.test.assertSame

class CalendarEtlJdbcConfigurationTest {

    private val configuration = CalendarEtlJdbcConfiguration()

    @Test
    fun `creates distinct datasource beans for ingest read and service write`() {
        val baseProperties = DataSourceProperties().apply {
            url = "jdbc:postgresql://localhost:5432/personal"
            username = "postgres"
            password = "postgres"
            driverClassName = "org.postgresql.Driver"
        }
        val overrides = CalendarEtlDataSourceProperties()

        val defaultDataSource = configuration.dataSource(baseProperties) as HikariDataSource
        val ingestDataSource = configuration.ingestReadDataSource(baseProperties, overrides) as HikariDataSource
        val serviceDataSource = configuration.serviceDataSource(baseProperties, overrides) as HikariDataSource

        try {
            assertNotSame(defaultDataSource, ingestDataSource)
            assertNotSame(defaultDataSource, serviceDataSource)
            assertNotSame(ingestDataSource, serviceDataSource)
            assertSame(ingestDataSource, configuration.ingestReadJdbcTemplate(ingestDataSource).dataSource)
            assertSame(serviceDataSource, configuration.serviceJdbcTemplate(serviceDataSource).dataSource)

            val transactionManager =
                configuration.serviceEtlTransactionManager(serviceDataSource) as DataSourceTransactionManager
            assertSame(serviceDataSource, transactionManager.dataSource)
            assertSame(
                transactionManager,
                configuration.serviceEtlTransactionTemplate(transactionManager).transactionManager,
            )
        } finally {
            defaultDataSource.close()
            ingestDataSource.close()
            serviceDataSource.close()
        }
    }

    @Test
    fun `applies ingest and service datasource overrides independently`() {
        val baseProperties = DataSourceProperties().apply {
            url = "jdbc:postgresql://localhost:5432/personal"
            username = "postgres"
            password = "postgres"
            driverClassName = "org.postgresql.Driver"
        }
        val overrides = CalendarEtlDataSourceProperties(
            ingestReadDatasource = CalendarEtlDataSourceOverride(
                url = "jdbc:postgresql://localhost:5432/ingest_read",
                username = "ingest_user",
            ),
            serviceDatasource = CalendarEtlDataSourceOverride(
                url = "jdbc:postgresql://localhost:5432/service_write",
                username = "service_user",
            ),
        )

        val ingestDataSource = configuration.ingestReadDataSource(baseProperties, overrides) as HikariDataSource
        val serviceDataSource = configuration.serviceDataSource(baseProperties, overrides) as HikariDataSource

        try {
            assertEquals("jdbc:postgresql://localhost:5432/ingest_read", ingestDataSource.jdbcUrl)
            assertEquals("ingest_user", ingestDataSource.username)
            assertEquals("jdbc:postgresql://localhost:5432/service_write", serviceDataSource.jdbcUrl)
            assertEquals("service_user", serviceDataSource.username)
        } finally {
            ingestDataSource.close()
            serviceDataSource.close()
        }
    }
}
