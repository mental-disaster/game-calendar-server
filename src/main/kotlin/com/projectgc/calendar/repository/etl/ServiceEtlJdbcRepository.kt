package com.projectgc.calendar.repository.etl

import com.projectgc.calendar.service.etl.ServiceEtlTrigger
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.ResultSetExtractor
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

@Repository
class ServiceEtlJdbcRepository(
    private val jdbc: JdbcTemplate,
) {
    fun insertRunLog(runId: UUID, trigger: ServiceEtlTrigger, startedAt: Instant) {
        jdbc.update(
            """
            INSERT INTO service.etl_run_log
                (run_id, trigger_type, ingest_sync_id, started_at)
            VALUES (?, ?, ?, ?)
            """.trimIndent(),
            runId,
            trigger.type.name.lowercase(),
            trigger.ingestSyncId,
            Timestamp.from(startedAt),
        )
    }

    fun finishRunLog(
        runId: UUID,
        finishedAt: Instant,
        status: String,
        mismatchCount: Int = 0,
        errorMessage: String? = null,
    ) {
        jdbc.update(
            """
            UPDATE service.etl_run_log
            SET finished_at = ?, status = ?, mismatch_count = ?, error_message = ?
            WHERE run_id = ?
            """.trimIndent(),
            Timestamp.from(finishedAt),
            status,
            mismatchCount,
            errorMessage,
            runId,
        )
    }

    fun findCursor(tableName: String): Long? =
        jdbc.query(
            "SELECT last_synced_at FROM service.etl_cursor WHERE table_name = ?",
            ResultSetExtractor { rs -> if (rs.next()) rs.getLong("last_synced_at") else null },
            tableName,
        )

    fun insertSourceLog(entry: ServiceEtlSourceLogEntry) {
        jdbc.update(
            """
            INSERT INTO service.etl_source_log
                (run_id, table_name, status, processed_rows, cursor_from, cursor_to, note, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """.trimIndent(),
            entry.runId,
            entry.tableName,
            entry.status,
            entry.processedRows,
            entry.cursorFrom,
            entry.cursorTo,
            entry.note,
            Timestamp.from(entry.startedAt),
            Timestamp.from(entry.finishedAt),
        )
    }
}

data class ServiceEtlSourceLogEntry(
    val runId: UUID,
    val tableName: String,
    val status: String,
    val processedRows: Int,
    val cursorFrom: Long?,
    val cursorTo: Long?,
    val note: String?,
    val startedAt: Instant,
    val finishedAt: Instant,
)
