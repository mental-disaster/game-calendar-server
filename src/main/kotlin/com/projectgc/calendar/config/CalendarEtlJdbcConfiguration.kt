package com.projectgc.calendar.config

import com.zaxxer.hikari.HikariDataSource
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import javax.sql.DataSource

@Configuration
@EnableConfigurationProperties(CalendarEtlDataSourceProperties::class)
class CalendarEtlJdbcConfiguration {

    @Bean
    @Primary
    fun dataSource(dataSourceProperties: DataSourceProperties): DataSource =
        buildDataSource(
            url = dataSourceProperties.determineUrl(),
            username = dataSourceProperties.determineUsername(),
            password = dataSourceProperties.determinePassword(),
            driverClassName = dataSourceProperties.determineDriverClassName(),
        )

    @Bean("ingestReadDataSource")
    fun ingestReadDataSource(
        dataSourceProperties: DataSourceProperties,
        calendarEtlDataSourceProperties: CalendarEtlDataSourceProperties,
    ): DataSource {
        val override = calendarEtlDataSourceProperties.ingestReadDatasource
        return buildDataSource(
            url = override.url ?: dataSourceProperties.determineUrl(),
            username = override.username ?: dataSourceProperties.determineUsername(),
            password = override.password ?: dataSourceProperties.determinePassword(),
            driverClassName = override.driverClassName ?: dataSourceProperties.determineDriverClassName(),
        )
    }

    @Bean("serviceDataSource")
    fun serviceDataSource(
        dataSourceProperties: DataSourceProperties,
        calendarEtlDataSourceProperties: CalendarEtlDataSourceProperties,
    ): DataSource {
        val override = calendarEtlDataSourceProperties.serviceDatasource
        return buildDataSource(
            url = override.url ?: dataSourceProperties.determineUrl(),
            username = override.username ?: dataSourceProperties.determineUsername(),
            password = override.password ?: dataSourceProperties.determinePassword(),
            driverClassName = override.driverClassName ?: dataSourceProperties.determineDriverClassName(),
        )
    }

    @Bean("serviceJdbcTemplate")
    fun serviceJdbcTemplate(
        @Suppress("SpringJavaInjectionPointsAutowiringInspection")
        @Qualifier("serviceDataSource")
        dataSource: DataSource,
    ) =
        JdbcTemplate(dataSource)

    @Bean("ingestReadJdbcTemplate")
    fun ingestReadJdbcTemplate(
        @Suppress("SpringJavaInjectionPointsAutowiringInspection")
        @Qualifier("ingestReadDataSource")
        dataSource: DataSource,
    ) =
        JdbcTemplate(dataSource)

    @Bean
    @Primary
    fun jdbcTemplate(dataSource: DataSource) = JdbcTemplate(dataSource)

    @Bean("serviceEtlTransactionManager")
    fun serviceEtlTransactionManager(
        @Suppress("SpringJavaInjectionPointsAutowiringInspection")
        @Qualifier("serviceDataSource")
        dataSource: DataSource,
    ): PlatformTransactionManager = DataSourceTransactionManager(dataSource)

    @Bean("serviceEtlTransactionTemplate")
    fun serviceEtlTransactionTemplate(
        @Suppress("SpringJavaInjectionPointsAutowiringInspection")
        @Qualifier("serviceEtlTransactionManager")
        transactionManager: PlatformTransactionManager,
    ) = TransactionTemplate(transactionManager)

    private fun buildDataSource(
        url: String,
        username: String?,
        password: String?,
        driverClassName: String?,
    ): DataSource = HikariDataSource().apply {
        jdbcUrl = url
        this.username = username
        this.password = password
        if (!driverClassName.isNullOrBlank()) {
            this.driverClassName = driverClassName
        }
    }
}

@ConfigurationProperties("calendar.etl")
data class CalendarEtlDataSourceProperties(
    var ingestReadDatasource: CalendarEtlDataSourceOverride = CalendarEtlDataSourceOverride(),
    var serviceDatasource: CalendarEtlDataSourceOverride = CalendarEtlDataSourceOverride(),
)

data class CalendarEtlDataSourceOverride(
    var url: String? = null,
    var username: String? = null,
    var password: String? = null,
    var driverClassName: String? = null,
)
