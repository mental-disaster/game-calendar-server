package com.projectgc.batch.service

import com.projectgc.batch.client.IgdbClient
import com.projectgc.batch.client.IgdbClient.Companion.PAGE_SIZE
import com.projectgc.batch.models.mapper.*
import com.projectgc.batch.repository.ingest.IngestRepositories
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant

// TODO(MQ): MQ 도입 시 각 syncXxxChunk 메서드를 @RabbitListener / @KafkaListener Consumer로 전환
//           GameReleaseBatchService의 청크 loop는 큐에 메시지를 publish하는 Producer로 교체
@Service
class MediaSyncService(
    private val igdbClient: IgdbClient,
    private val repos: IngestRepositories,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun syncCoverChunk(gameIds: List<Long>, stats: TableSyncStats) = syncChunk(gameIds, stats) { ids, lastId ->
        val result = igdbClient.fetchCoversByGameIds(ids, lastId)
        stats.fetched += result.fetched
        stats.upserted += result.items.size
        stats.parseErrors += result.errors
        repos.jdbc.upsertCovers(result.items.map { it.toEntity() })
        if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
    }

    fun syncArtworkChunk(gameIds: List<Long>, stats: TableSyncStats) = syncChunk(gameIds, stats) { ids, lastId ->
        val result = igdbClient.fetchArtworksByGameIds(ids, lastId)
        stats.fetched += result.fetched
        stats.upserted += result.items.size
        stats.parseErrors += result.errors
        repos.jdbc.upsertArtworks(result.items.map { it.toArtworkEntity() })
        if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
    }

    fun syncScreenshotChunk(gameIds: List<Long>, stats: TableSyncStats) = syncChunk(gameIds, stats) { ids, lastId ->
        val result = igdbClient.fetchScreenshotsByGameIds(ids, lastId)
        stats.fetched += result.fetched
        stats.upserted += result.items.size
        stats.parseErrors += result.errors
        repos.jdbc.upsertScreenshots(result.items.map { it.toScreenshotEntity() })
        if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
    }

    fun syncGameVideoChunk(gameIds: List<Long>, stats: TableSyncStats) = syncChunk(gameIds, stats) { ids, lastId ->
        val result = igdbClient.fetchGameVideosByGameIds(ids, lastId)
        stats.fetched += result.fetched
        stats.upserted += result.items.size
        stats.parseErrors += result.errors
        repos.jdbc.upsertGameVideos(result.items.map { it.toEntity() })
        if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
    }

    fun syncWebsiteChunk(gameIds: List<Long>, stats: TableSyncStats) = syncChunk(gameIds, stats) { ids, lastId ->
        val result = igdbClient.fetchWebsitesByGameIds(ids, lastId)
        stats.fetched += result.fetched
        stats.upserted += result.items.size
        stats.parseErrors += result.errors
        repos.jdbc.upsertWebsites(result.items.map { it.toEntity() })
        if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
    }

    fun syncAlternativeNameChunk(gameIds: List<Long>, stats: TableSyncStats) = syncChunk(gameIds, stats) { ids, lastId ->
        val result = igdbClient.fetchAlternativeNamesByGameIds(ids, lastId)
        stats.fetched += result.fetched
        stats.upserted += result.items.size
        stats.parseErrors += result.errors
        repos.jdbc.upsertAlternativeNames(result.items.map { it.toEntity() })
        if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
    }

    companion object {
        private const val MAX_LOOP_GUARD = 10_000
    }

    // [A] Keyset Pagination — 람다가 다음 lastId 또는 null(완료) 반환
    private fun syncChunk(
        gameIds: List<Long>,
        stats: TableSyncStats,
        fetchAndSave: (List<Long>, Long) -> Long?,
    ) {
        var lastId = 0L
        var iterations = 0
        while (true) {
            if (++iterations > MAX_LOOP_GUARD) {
                log.error("[${stats.tableName}] 최대 반복 횟수($MAX_LOOP_GUARD) 초과 — 루프 강제 종료")
                break
            }
            lastId = fetchAndSave(gameIds, lastId) ?: break
        }
        log.debug("[${stats.tableName}] 청크 완료 (gameIds=${gameIds.size}개)")
    }
}
