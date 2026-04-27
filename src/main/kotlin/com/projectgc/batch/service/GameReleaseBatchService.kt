package com.projectgc.batch.service

import com.projectgc.batch.client.FetchResult
import com.projectgc.batch.client.IgdbClient
import com.projectgc.batch.client.IgdbClient.Companion.PAGE_SIZE
import com.projectgc.batch.models.entity.ingest.IngestSyncCursorEntity
import com.projectgc.batch.models.mapper.*
import com.projectgc.batch.repository.ingest.IngestRepositories
import com.projectgc.shared.event.IngestSyncSucceededEvent
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.data.domain.PageRequest
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant
import java.util.UUID

@Service
class GameReleaseBatchService(
    private val igdbClient: IgdbClient,
    private val mediaSync: MediaSyncService,
    private val repos: IngestRepositories,
    private val eventPublisher: ApplicationEventPublisher,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    private class PartialIngestSyncFailureException(
        val failedTables: List<String>,
    ) : RuntimeException("IGDB 동기화 부분 실패: ${failedTables.joinToString(", ")}")

    private class LoopGuardExceededException(
        tableName: String,
        guard: Int,
        scope: String,
    ) : RuntimeException("[$tableName] $scope 최대 반복 횟수($guard) 초과")

    companion object {
        // [K] PAGE_SIZE(IGDB 응답 한도)와 동일하게 설정 — 청크당 최대 1페이지로 직관적
        //     의미는 다름: PAGE_SIZE = API 결과 한도 / MEDIA_CHUNK_SIZE = DB game ID 청크 크기
        private const val MEDIA_CHUNK_SIZE = PAGE_SIZE
        // 10,000 × 500건 = 500만건: 현실적으로 도달 불가한 상한 — IGDB 응답 이상 시 무한 루프 방지
        private const val MAX_LOOP_GUARD = 10_000
    }

    // TODO(MQ): MQ 도입 시 아래 미디어 청크 loop를 큐에 메시지를 publish하는 Producer로 교체
    @Async("batchTaskExecutor")
    fun syncAll() {
        // [B] 커서를 sync 종료 시각이 아닌 시작 시각으로 설정
        //     sync 도중 IGDB에서 변경된 레코드도 다음 sync에서 반드시 잡힘
        val syncStartedAt = Instant.now().epochSecond
        val startTime = Instant.now()
        val syncId = UUID.randomUUID()
        var syncLogInserted = false
        var successEvent: IngestSyncSucceededEvent? = null
        log.info("IGDB 전체 동기화 시작 (syncId=$syncId)")

        try {
            repos.jdbc.insertSyncLog(syncId, startTime)
            syncLogInserted = true

            val failedTables = mutableListOf<String>()

            // 1. 참조 테이블 (소량, 단일 호출)
            captureTableFailure(failedTables, "genre") { syncReference("genre", syncId, syncStartedAt, { igdbClient.fetchGenres() }) { r -> repos.jdbc.upsertGenres(r.items.map { it.toGenreEntity() }) } }
            captureTableFailure(failedTables, "theme") { syncReference("theme", syncId, syncStartedAt, { igdbClient.fetchThemes() }) { r -> repos.jdbc.upsertThemes(r.items.map { it.toThemeEntity() }) } }
            captureTableFailure(failedTables, "player_perspective") { syncReference("player_perspective", syncId, syncStartedAt, { igdbClient.fetchPlayerPerspectives() }) { r -> repos.jdbc.upsertPlayerPerspectives(r.items.map { it.toPlayerPerspectiveEntity() }) } }
            captureTableFailure(failedTables, "game_mode") { syncReference("game_mode", syncId, syncStartedAt, { igdbClient.fetchGameModes() }) { r -> repos.jdbc.upsertGameModes(r.items.map { it.toGameModeEntity() }) } }
            captureTableFailure(failedTables, "language_support_type") { syncReference("language_support_type", syncId, syncStartedAt, { igdbClient.fetchLanguageSupportTypes() }) { r -> repos.jdbc.upsertLanguageSupportTypes(r.items.map { it.toLanguageSupportTypeEntity() }) } }
            captureTableFailure(failedTables, "platform_type") { syncReference("platform_type", syncId, syncStartedAt, { igdbClient.fetchPlatformTypes() }) { r -> repos.jdbc.upsertPlatformTypes(r.items.map { it.toPlatformTypeEntity() }) } }
            captureTableFailure(failedTables, "website_type") { syncReference("website_type", syncId, syncStartedAt, { igdbClient.fetchWebsiteTypes() }) { r -> repos.jdbc.upsertWebsiteTypes(r.items.map { it.toEntity() }) } }
            captureTableFailure(failedTables, "game_status") { syncReference("game_status", syncId, syncStartedAt, { igdbClient.fetchGameStatuses() }) { r -> repos.jdbc.upsertGameStatuses(r.items.map { it.toEntity() }) } }
            captureTableFailure(failedTables, "game_type") { syncReference("game_type", syncId, syncStartedAt, { igdbClient.fetchGameTypes() }) { r -> repos.jdbc.upsertGameTypes(r.items.map { it.toEntity() }) } }
            captureTableFailure(failedTables, "language") { syncReference("language", syncId, syncStartedAt, { igdbClient.fetchLanguages() }) { r -> repos.jdbc.upsertLanguages(r.items.map { it.toEntity() }) } }
            captureTableFailure(failedTables, "region") { syncReference("region", syncId, syncStartedAt, { igdbClient.fetchRegions() }) { r -> repos.jdbc.upsertRegions(r.items.map { it.toEntity() }) } }
            captureTableFailure(failedTables, "release_date_region") { syncReference("release_date_region", syncId, syncStartedAt, { igdbClient.fetchReleaseDateRegions() }) { r -> repos.jdbc.upsertReleaseDateRegions(r.items.map { it.toEntity() }) } }
            captureTableFailure(failedTables, "release_date_status") { syncReference("release_date_status", syncId, syncStartedAt, { igdbClient.fetchReleaseDateStatuses() }) { r -> repos.jdbc.upsertReleaseDateStatuses(r.items.map { it.toEntity() }) } }

            // 2. game — 미디어 동기화가 DB의 game ID에 의존하므로 반드시 먼저 실행
            captureTableFailure(failedTables, "game") {
                syncWithCursor("game", syncId, syncStartedAt) { cursor, lastId ->
                    val result = igdbClient.fetchGames(cursor, lastId)
                    fetched += result.fetched
                    upserted += result.items.size
                    parseErrors += result.errors
                    repos.jdbc.upsertGames(result.items.map { it.toEntity() })
                    if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
                }
            }

            // 3. 나머지 커서 기반 (updated_at 있음)
            captureTableFailure(failedTables, "release_date") {
                syncWithCursor("release_date", syncId, syncStartedAt) { cursor, lastId ->
                    val result = igdbClient.fetchReleaseDates(cursor, lastId)
                    fetched += result.fetched
                    upserted += result.items.size
                    parseErrors += result.errors
                    repos.jdbc.upsertReleaseDates(result.items.map { it.toEntity() })
                    if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
                }
            }
            captureTableFailure(failedTables, "platform") {
                syncWithCursor("platform", syncId, syncStartedAt) { cursor, lastId ->
                    val result = igdbClient.fetchPlatforms(cursor, lastId)
                    fetched += result.fetched
                    upserted += result.items.size
                    parseErrors += result.errors
                    repos.jdbc.upsertPlatforms(result.items.map { it.toEntity() })
                    if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
                }
            }
            captureTableFailure(failedTables, "involved_company") {
                syncWithCursor("involved_company", syncId, syncStartedAt) { cursor, lastId ->
                    val result = igdbClient.fetchInvolvedCompanies(cursor, lastId)
                    fetched += result.fetched
                    upserted += result.items.size
                    parseErrors += result.errors
                    repos.jdbc.upsertInvolvedCompanies(result.items.map { it.toEntity() })
                    if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
                }
            }
            captureTableFailure(failedTables, "company") { syncCompaniesByInvolvedCompanyIds(syncId, syncStartedAt) }
            captureTableFailure(failedTables, "language_support") {
                syncWithCursor("language_support", syncId, syncStartedAt) { cursor, lastId ->
                    val result = igdbClient.fetchLanguageSupports(cursor, lastId)
                    fetched += result.fetched
                    upserted += result.items.size
                    parseErrors += result.errors
                    repos.jdbc.upsertLanguageSupports(result.items.map { it.toEntity() })
                    if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
                }
            }
            captureTableFailure(failedTables, "game_localization") {
                syncWithCursor("game_localization", syncId, syncStartedAt) { cursor, lastId ->
                    val result = igdbClient.fetchGameLocalizations(cursor, lastId)
                    fetched += result.fetched
                    upserted += result.items.size
                    parseErrors += result.errors
                    repos.jdbc.upsertGameLocalizations(result.items.map { it.toEntity() })
                    if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
                }
            }

            // 4. 게임 ID 필터 기반 미디어
            captureTableFailure(failedTables, "cover") { syncMediaByGameIds("cover", syncId, syncStartedAt, mediaSync::syncCoverChunk) }
            captureTableFailure(failedTables, "artwork") { syncMediaByGameIds("artwork", syncId, syncStartedAt, mediaSync::syncArtworkChunk) }
            captureTableFailure(failedTables, "screenshot") { syncMediaByGameIds("screenshot", syncId, syncStartedAt, mediaSync::syncScreenshotChunk) }
            captureTableFailure(failedTables, "game_video") { syncMediaByGameIds("game_video", syncId, syncStartedAt, mediaSync::syncGameVideoChunk) }
            captureTableFailure(failedTables, "website") { syncMediaByGameIds("website", syncId, syncStartedAt, mediaSync::syncWebsiteChunk) }
            captureTableFailure(failedTables, "alternative_name") { syncMediaByGameIds("alternative_name", syncId, syncStartedAt, mediaSync::syncAlternativeNameChunk) }

            // 5. 전체 페이지네이션 (updated_at 없음 + game FK 없음)
            captureTableFailure(failedTables, "keyword") {
                syncPaginated("keyword", syncId, syncStartedAt) { lastId ->
                    val result = igdbClient.fetchKeywords(lastId)
                    fetched += result.fetched
                    upserted += result.items.size
                    parseErrors += result.errors
                    repos.jdbc.upsertKeywords(result.items.map { it.toKeywordEntity() })
                    if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
                }
            }
            captureTableFailure(failedTables, "platform_logo") {
                syncPaginated("platform_logo", syncId, syncStartedAt) { lastId ->
                    val result = igdbClient.fetchPlatformLogos(lastId)
                    fetched += result.fetched
                    upserted += result.items.size
                    parseErrors += result.errors
                    repos.jdbc.upsertPlatformLogos(result.items.map { it.toEntity() })
                    if (result.fetched == PAGE_SIZE) result.items.lastOrNull()?.id else null
                }
            }

            if (failedTables.isNotEmpty()) {
                throw PartialIngestSyncFailureException(failedTables.toList())
            }

            val finishedAt = Instant.now()
            repos.jdbc.finishSyncLog(syncId, finishedAt, "completed")
            successEvent = IngestSyncSucceededEvent(syncId = syncId, completedAt = finishedAt)
        } catch (ex: Exception) {
            when (ex) {
                is PartialIngestSyncFailureException -> log.warn(ex.message)
                else -> log.error("IGDB 전체 동기화 실패: ${ex.message}", ex)
            }

            if (syncLogInserted) {
                runCatching { repos.jdbc.finishSyncLog(syncId, Instant.now(), "failed") }
                    .onFailure { finishError ->
                        log.error("IGDB 실패 상태 기록 실패 (syncId=$syncId): ${finishError.message}", finishError)
                    }
            }
        }

        successEvent?.let(eventPublisher::publishEvent)

        // [J] Duration.between으로 소요 시간 계산
        log.info("IGDB 전체 동기화 완료 (syncId=$syncId, 소요: ${Duration.between(startTime, Instant.now()).toSeconds()}초)")
    }

    private fun captureTableFailure(
        failedTables: MutableList<String>,
        tableName: String,
        block: () -> Unit,
    ) {
        runCatching(block).onFailure {
            failedTables += tableName
            log.error("[$tableName] 동기화 실패: ${it.message}", it)
        }
    }

    // 소량 참조 테이블 — 단일 호출
    private fun <T> syncReference(
        tableName: String,
        syncId: UUID,
        syncStartedAt: Long,
        fetch: () -> FetchResult<T>,
        save: (FetchResult<T>) -> Unit,
    ) {
        log.info("[$tableName] 참조 동기화 시작")
        val stats = TableSyncStats(syncId, tableName)
        val result = fetch()
        stats.fetched += result.fetched
        stats.upserted += result.items.size
        stats.parseErrors += result.errors
        save(result)
        stats.finishedAt = Instant.now()
        updateCursor(tableName, syncStartedAt)
        saveStats(stats)
    }

    // [A] Keyset Pagination — lastId 추적, 람다가 다음 lastId 또는 null(완료) 반환
    private fun syncWithCursor(tableName: String, syncId: UUID, syncStartedAt: Long, fetchAndSave: TableSyncStats.(Long, Long) -> Long?) {
        val cursor = repos.syncCursor.findById(tableName).map { it.lastSyncedAt }.orElse(0L)
        log.info("[$tableName] 커서 기반 동기화 시작 (cursor=$cursor)")
        val stats = TableSyncStats(syncId, tableName)
        var lastId = 0L
        var iterations = 0
        while (true) {
            if (++iterations > MAX_LOOP_GUARD) {
                throw LoopGuardExceededException(
                    tableName = tableName,
                    guard = MAX_LOOP_GUARD,
                    scope = "커서 기반 동기화",
                )
            }
            lastId = stats.fetchAndSave(cursor, lastId) ?: break
        }
        stats.finishedAt = Instant.now()
        updateCursor(tableName, syncStartedAt)
        saveStats(stats)
    }

    // [A] Keyset Pagination — 전체 순회 (updated_at 없음 + game FK 없음)
    private fun syncPaginated(tableName: String, syncId: UUID, syncStartedAt: Long, fetchAndSave: TableSyncStats.(Long) -> Long?) {
        log.info("[$tableName] 전체 페이지네이션 동기화 시작")
        val stats = TableSyncStats(syncId, tableName)
        var lastId = 0L
        var iterations = 0
        while (true) {
            if (++iterations > MAX_LOOP_GUARD) {
                throw LoopGuardExceededException(
                    tableName = tableName,
                    guard = MAX_LOOP_GUARD,
                    scope = "전체 페이지네이션",
                )
            }
            lastId = stats.fetchAndSave(lastId) ?: break
        }
        stats.finishedAt = Instant.now()
        updateCursor(tableName, syncStartedAt)
        saveStats(stats)
    }

    // [F] DB game ID Keyset + 미디어 커서 기반 증분 필터
    // - 초기 수집(cursor=0): 전체 게임 ID 순회
    // - 증분 수집(cursor>0): 미디어 커서 이후 updatedAt이 변경된 게임만 순회
    //   (IGDB는 자식 미디어 변경 시 부모 game.updatedAt도 갱신)
    private fun syncMediaByGameIds(tableName: String, syncId: UUID, syncStartedAt: Long, syncChunk: (List<Long>, TableSyncStats) -> Unit) {
        val mediaCursor = repos.syncCursor.findById(tableName).map { it.lastSyncedAt }.orElse(0L)
        log.info("[$tableName] 게임 ID 필터 동기화 시작 (mediaCursor=$mediaCursor)")
        val stats = TableSyncStats(syncId, tableName)
        var lastGameId = 0L
        var iterations = 0
        do {
            if (++iterations > MAX_LOOP_GUARD) {
                throw LoopGuardExceededException(
                    tableName = tableName,
                    guard = MAX_LOOP_GUARD,
                    scope = "게임 ID 순회",
                )
            }
            val slice = if (mediaCursor > 0L) {
                repos.game.findAllIdsUpdatedAfter(lastGameId, mediaCursor, PageRequest.of(0, MEDIA_CHUNK_SIZE))
            } else {
                repos.game.findAllIdsAfter(lastGameId, PageRequest.of(0, MEDIA_CHUNK_SIZE))
            }
            slice.content.takeIf { it.isNotEmpty() }?.let {
                syncChunk(it, stats)
                lastGameId = it.last()
            }
        } while (slice.hasNext())
        updateCursor(tableName, syncStartedAt)
        stats.finishedAt = Instant.now()
        saveStats(stats)
    }

    // involved_company에서 추출한 company ID 기반으로 청크 분할 후 수집
    private fun syncCompaniesByInvolvedCompanyIds(syncId: UUID, syncStartedAt: Long) {
        val cursor = repos.syncCursor.findById("company").map { it.lastSyncedAt }.orElse(0L)
        val companyIds = repos.involvedCompany.findAllDistinctCompanyIds()
        log.info("[company] involved_company 기반 동기화 시작 (companyIds=${companyIds.size}개, cursor=$cursor)")
        val stats = TableSyncStats(syncId, "company")
        companyIds.chunked(PAGE_SIZE).forEach { chunk ->
            var lastId = 0L
            var iterations = 0
            while (true) {
                if (++iterations > MAX_LOOP_GUARD) {
                    throw LoopGuardExceededException(
                        tableName = "company",
                        guard = MAX_LOOP_GUARD,
                        scope = "company keyset 순회",
                    )
                }
                val result = igdbClient.fetchCompaniesByIds(chunk, cursor, lastId)
                stats.fetched += result.fetched
                stats.upserted += result.items.size
                stats.parseErrors += result.errors
                repos.jdbc.upsertCompanies(result.items.map { it.toEntity() })
                val nextId = result.items.lastOrNull()?.id
                if (result.fetched == PAGE_SIZE && nextId != null) lastId = nextId else break
            }
        }
        stats.finishedAt = Instant.now()
        updateCursor("company", syncStartedAt)
        saveStats(stats)
    }

    // [D] findById 제거 — save(merge)가 upsert 처리하므로 SELECT 불필요
    // [B] syncStartedAt: sync 시작 시각을 커서로 사용 (sync 도중 변경분 누락 방지)
    // [I] syncedAt: 커서 갱신 시각 기록
    private fun updateCursor(tableName: String, syncStartedAt: Long) {
        repos.syncCursor.save(IngestSyncCursorEntity().also {
            it.tableName = tableName
            it.lastSyncedAt = syncStartedAt
            it.syncedAt = Instant.now()
        })
    }

    private fun saveStats(stats: TableSyncStats) {
        runCatching {
            repos.jdbc.saveSyncTableLog(stats)
            if (stats.parseErrors.isNotEmpty()) {
                repos.jdbc.batchSaveSyncParseErrors(stats)
            }
        }.onFailure { log.error("[${stats.tableName}] 동기화 로그 저장 실패: ${it.message}", it) }
    }
}
