package com.projectgc.batch.client

import com.fasterxml.jackson.databind.ObjectMapper
import com.projectgc.batch.config.IgdbProperties
import com.projectgc.batch.models.dto.igdb.*
import org.slf4j.LoggerFactory
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.client.ResourceAccessException
import org.springframework.web.client.RestClient

@Component
class IgdbClient(
    private val igdbProperties: IgdbProperties,
    private val twitchAuthClient: TwitchAuthClient,
    private val objectMapper: ObjectMapper,
    restClientBuilder: RestClient.Builder,  // 테스트 시 MockRestServiceServer 주입을 위해 Builder 주입
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val restClient = restClientBuilder.build()

    companion object {
        private const val RATE_LIMIT_DELAY_MS = 300L
        private const val MAX_RETRY_ATTEMPTS = 3
        private const val BASE_RETRY_DELAY_MS = 1000L
        private const val RETRY_AFTER_FALLBACK_MS = 5000L
        const val PAGE_SIZE = 500

        private val GAME_FIELDS = """
            id, name, slug, summary, storyline,
            first_release_date, release_dates, platforms,
            game_status, game_type, language_supports,
            genres, themes, player_perspectives, game_modes, keywords,
            involved_companies, parent_game, remakes, remasters,
            ports, standalone_expansions, similar_games,
            cover, artworks, screenshots, videos, websites,
            alternative_names, game_localizations,
            tags, checksum, updated_at
        """.trimIndent()

        private val RELEASE_DATE_FIELDS =
            "id, game, platform, region, status, date, y, m, human, checksum, updated_at"

        private val PLATFORM_FIELDS =
            "id, name, abbreviation, alternative_name, platform_logo, platform_type, checksum, updated_at"

        private val COMPANY_FIELDS =
            "id, name, parent, changed_company_id, developed, published, checksum, updated_at"

        private val INVOLVED_COMPANY_FIELDS =
            "id, game, company, developer, publisher, porting, supporting, checksum, updated_at"

        private val COVER_FIELDS =
            "id, game, game_localization, image_id, url, checksum"

        private val LANGUAGE_SUPPORT_FIELDS =
            "id, game, language, language_support_type, checksum, updated_at"

        private val GAME_LOCALIZATION_FIELDS =
            "id, game, region, name, cover, checksum, updated_at"
    }

    // ============================================================
    // 핵심 엔드포인트 — 커서 기반 (updated_at + Keyset)
    // ============================================================

    fun fetchGames(updatedAfter: Long, lastId: Long): FetchResult<IgdbGameDto> =
        postWithRetry("/games", buildGamesQuery(updatedAfter, lastId))

    private fun buildGamesQuery(updatedAfter: Long, lastId: Long): String =
        buildQuery(
            fields = GAME_FIELDS,
            where = gameFilterCondition() + updatedAtCondition(updatedAfter),
            lastId = lastId,
        )

    fun fetchReleaseDates(updatedAfter: Long, lastId: Long): FetchResult<IgdbReleaseDateDto> =
        postWithRetry("/release_dates", buildGameScopedQuery(RELEASE_DATE_FIELDS, updatedAfter, lastId))

    fun fetchPlatforms(updatedAfter: Long, lastId: Long): FetchResult<IgdbPlatformDto> =
        postWithRetry("/platforms", buildQuery(PLATFORM_FIELDS, updatedAfter = updatedAfter, lastId = lastId))

    fun fetchInvolvedCompanies(updatedAfter: Long, lastId: Long): FetchResult<IgdbInvolvedCompanyDto> =
        postWithRetry("/involved_companies", buildGameScopedQuery(INVOLVED_COMPANY_FIELDS, updatedAfter, lastId))

    fun fetchCompaniesByIds(companyIds: List<Long>, updatedAfter: Long, lastId: Long): FetchResult<IgdbCompanyDto> =
        postWithRetry("/companies", buildIdScopedQuery(COMPANY_FIELDS, companyIds, updatedAfter, lastId))

    fun fetchLanguageSupports(updatedAfter: Long, lastId: Long): FetchResult<IgdbLanguageSupportDto> =
        postWithRetry("/language_supports", buildGameScopedQuery(LANGUAGE_SUPPORT_FIELDS, updatedAfter, lastId))

    fun fetchGameLocalizations(updatedAfter: Long, lastId: Long): FetchResult<IgdbGameLocalizationDto> =
        postWithRetry("/game_localizations", buildGameScopedQuery(GAME_LOCALIZATION_FIELDS, updatedAfter, lastId))

    // ============================================================
    // 참조 테이블 (소량, 단일 호출)
    // ============================================================

    fun fetchGenres(): FetchResult<IgdbNamedDto> =
        postWithRetry("/genres", buildQuery("id, name, checksum, updated_at"))

    fun fetchThemes(): FetchResult<IgdbNamedDto> =
        postWithRetry("/themes", buildQuery("id, name, checksum, updated_at"))

    fun fetchPlayerPerspectives(): FetchResult<IgdbNamedDto> =
        postWithRetry("/player_perspectives", buildQuery("id, name, checksum, updated_at"))

    fun fetchGameModes(): FetchResult<IgdbNamedDto> =
        postWithRetry("/game_modes", buildQuery("id, name, checksum, updated_at"))

    fun fetchLanguages(): FetchResult<IgdbLanguageDto> =
        postWithRetry("/languages", buildQuery("id, locale, name, native_name, checksum, updated_at"))

    fun fetchLanguageSupportTypes(): FetchResult<IgdbNamedDto> =
        postWithRetry("/language_support_types", buildQuery("id, name, checksum, updated_at"))

    fun fetchRegions(): FetchResult<IgdbRegionDto> =
        postWithRetry("/regions", buildQuery("id, name, identifier, checksum, updated_at"))

    fun fetchReleaseDateRegions(): FetchResult<IgdbReleaseDateRegionDto> =
        postWithRetry("/release_date_regions", buildQuery("id, region, checksum, updated_at"))

    fun fetchReleaseDateStatuses(): FetchResult<IgdbReleaseDateStatusDto> =
        postWithRetry("/release_date_statuses", buildQuery("id, name, description, checksum, updated_at"))

    fun fetchGameStatuses(): FetchResult<IgdbGameStatusDto> =
        postWithRetry("/game_statuses", buildQuery("id, status, checksum, updated_at"))

    fun fetchGameTypes(): FetchResult<IgdbGameTypeDto> =
        postWithRetry("/game_types", buildQuery("id, type, checksum, updated_at"))

    fun fetchPlatformTypes(): FetchResult<IgdbNamedDto> =
        postWithRetry("/platform_types", buildQuery("id, name, checksum, updated_at"))

    fun fetchWebsiteTypes(): FetchResult<IgdbWebsiteTypeDto> =
        postWithRetry("/website_types", buildQuery("id, type, checksum, updated_at"))

    // ============================================================
    // 전체 페이지네이션 (updated_at 없음 + game FK 없음) — Keyset
    // ============================================================

    fun fetchKeywords(lastId: Long): FetchResult<IgdbNamedDto> =
        postWithRetry("/keywords", buildQuery("id, name, checksum, updated_at", lastId = lastId))

    fun fetchPlatformLogos(lastId: Long): FetchResult<IgdbPlatformLogoDto> =
        postWithRetry("/platform_logos", buildQuery("id, image_id, url, checksum", lastId = lastId))

    // ============================================================
    // 게임 ID 필터 기반 미디어 (updated_at 없음 + game FK 있음) — Keyset
    // ============================================================

    fun fetchCoversByGameIds(gameIds: List<Long>, lastId: Long): FetchResult<IgdbCoverDto> =
        postWithRetry("/covers", buildGameFilteredQuery(COVER_FIELDS, gameIds, lastId))

    fun fetchArtworksByGameIds(gameIds: List<Long>, lastId: Long): FetchResult<IgdbImageDto> =
        postWithRetry("/artworks", buildGameFilteredQuery("id, game, image_id, url, checksum", gameIds, lastId))

    fun fetchScreenshotsByGameIds(gameIds: List<Long>, lastId: Long): FetchResult<IgdbImageDto> =
        postWithRetry("/screenshots", buildGameFilteredQuery("id, game, image_id, url, checksum", gameIds, lastId))

    fun fetchGameVideosByGameIds(gameIds: List<Long>, lastId: Long): FetchResult<IgdbGameVideoDto> =
        postWithRetry("/game_videos", buildGameFilteredQuery("id, game, name, video_id, checksum", gameIds, lastId))

    fun fetchWebsitesByGameIds(gameIds: List<Long>, lastId: Long): FetchResult<IgdbWebsiteDto> =
        postWithRetry("/websites", buildGameFilteredQuery("id, game, type, url, trusted, checksum", gameIds, lastId))

    fun fetchAlternativeNamesByGameIds(gameIds: List<Long>, lastId: Long): FetchResult<IgdbAlternativeNameDto> =
        postWithRetry("/alternative_names", buildGameFilteredQuery("id, game, name, comment, checksum", gameIds, lastId))

    // ============================================================
    // 내부 헬퍼
    // ============================================================

    // sort를 항상 id asc로 고정 — Keyset Pagination 기반
    // offset 제거: IGDB max offset 5000 제한 우회
    private fun buildQuery(
        fields: String,
        where: String? = null,
        updatedAfter: Long = 0L,
        lastId: Long = 0L,
    ): String = buildString {
        appendLine("fields $fields;")
        var condition = where ?: updatedAtCondition(updatedAfter).trimStart(' ', '&')
        if (lastId > 0) {
            condition = if (condition.isBlank()) "id > $lastId" else "$condition & id > $lastId"
        }
        if (condition.isNotBlank()) appendLine("where $condition;")
        appendLine("sort id asc;")
        appendLine("limit $PAGE_SIZE;")
    }

    // game FK 있는 테이블용 — buildGamesQuery와 동일한 스코프 조건을 game. prefix로 적용
    private fun buildGameScopedQuery(
        fields: String,
        updatedAfter: Long,
        lastId: Long,
        extraCondition: String? = null,
    ): String {
        var where = gameFilterCondition(prefix = "game")
        if (extraCondition != null) where += " & $extraCondition"
        where += updatedAtCondition(updatedAfter)
        return buildQuery(fields, where = where, lastId = lastId)
    }

    private fun gameFilterCondition(prefix: String = ""): String {
        val p = if (prefix.isNotEmpty()) "$prefix." else ""
        val gameTypes = igdbProperties.filter.gameTypes.joinToString(",")
        val minReleaseDate = igdbProperties.filter.minFirstReleaseDate
        val platformIds = igdbProperties.filter.platformIds
        var condition = "${p}first_release_date >= $minReleaseDate & ${p}game_type = ($gameTypes)"
        if (platformIds.isNotEmpty()) condition += " & ${p}platforms = (${platformIds.joinToString(",")})"
        return condition
    }

    // company ID 목록 기반 조회 — involved_company에서 추출한 ID만 수집
    private fun buildIdScopedQuery(fields: String, ids: List<Long>, updatedAfter: Long, lastId: Long): String {
        val where = "id = (${ids.joinToString(",")})" + updatedAtCondition(updatedAfter)
        return buildQuery(fields, where = where, lastId = lastId)
    }

    private fun buildGameFilteredQuery(fields: String, gameIds: List<Long>, lastId: Long): String = buildString {
        appendLine("fields $fields;")
        var condition = "game = (${gameIds.joinToString(",")})"
        if (lastId > 0) condition += " & id > $lastId"
        appendLine("where $condition;")
        appendLine("sort id asc;")
        appendLine("limit $PAGE_SIZE;")
    }

    private fun updatedAtCondition(updatedAfter: Long): String =
        if (updatedAfter > 0) " & updated_at > $updatedAfter" else ""

    // [G] inline 분리: postWithRetry는 TypeRef 캡처만 하는 thin wrapper (인라이닝해도 bloat 없음)
    //     실제 재시도 로직은 non-inline postWithRetryInternal에 한 번만 존재
    private inline fun <reified T> postWithRetry(endpoint: String, body: String): FetchResult<T> {
        val typeRef = object : ParameterizedTypeReference<List<T>>() {}
        return postWithRetryInternal(endpoint, body, typeRef)
    }

    // 재시도 + 지수적 백오프 (non-inline — 바이트코드 중복 없음)
    // - ResourceAccessException (Connection reset 등): 재시도
    // - 5xx: 재시도
    // - 429 Too Many Requests: Retry-After 헤더 파싱 후 대기, 재시도
    // - 401 Unauthorized: 토큰 캐시 무효화 후 재발급, 재시도
    // - 그 외 4xx: 즉시 throw
    private fun <T> postWithRetryInternal(
        endpoint: String,
        body: String,
        typeRef: ParameterizedTypeReference<List<T>>,
    ): FetchResult<T> {
        // 논리적 API 호출당 1회 적용 — 재시도는 별도 백오프로 충분하므로 추가 delay 불필요
        Thread.sleep(RATE_LIMIT_DELAY_MS)
        var lastException: Exception? = null

        repeat(MAX_RETRY_ATTEMPTS) { attempt ->
            try {
                return post(endpoint, body, typeRef)
            } catch (e: HttpClientErrorException.Unauthorized) {
                log.warn("[$endpoint] 401 Unauthorized — 토큰 갱신 후 재시도 (attempt=${attempt + 1})")
                twitchAuthClient.invalidateToken()
                lastException = e
            } catch (e: HttpClientErrorException.TooManyRequests) {
                val retryAfterMs = e.responseHeaders
                    ?.getFirst("Retry-After")
                    ?.toLongOrNull()
                    ?.let { it * 1000 }
                    ?: RETRY_AFTER_FALLBACK_MS
                log.warn("[$endpoint] 429 Too Many Requests — ${retryAfterMs}ms 대기 후 재시도 (attempt=${attempt + 1})")
                Thread.sleep(retryAfterMs)
                lastException = e
            } catch (e: HttpClientErrorException) {
                throw e  // 그 외 4xx는 재시도 없이 즉시 throw
            } catch (e: HttpServerErrorException) {
                val delayMs = BASE_RETRY_DELAY_MS shl attempt  // 1s → 2s → 4s
                log.warn("[$endpoint] 5xx 오류 — ${delayMs}ms 후 재시도 (attempt=${attempt + 1}): ${e.message}")
                Thread.sleep(delayMs)
                lastException = e
            } catch (e: ResourceAccessException) {
                val delayMs = BASE_RETRY_DELAY_MS shl attempt
                log.warn("[$endpoint] 네트워크 오류 — ${delayMs}ms 후 재시도 (attempt=${attempt + 1}): ${e.message}")
                Thread.sleep(delayMs)
                lastException = e
            }
        }

        throw lastException ?: IllegalStateException("[$endpoint] 재시도 실패")
    }

    // 레코드별 파싱: 1개 실패해도 나머지 살림
    // - 성공 레코드 → items
    // - 실패 레코드 → errors (raw JSON + 에러 메시지 보존)
    private fun <T> post(endpoint: String, body: String, typeRef: ParameterizedTypeReference<List<T>>): FetchResult<T> {
        val responseBody = restClient.post()
            .uri("${igdbProperties.baseUrl}$endpoint")
            .header("Client-ID", igdbProperties.clientId)
            .header("Authorization", "Bearer ${twitchAuthClient.getAccessToken()}")
            .contentType(MediaType.TEXT_PLAIN)
            .body(body)
            .retrieve()
            .body(String::class.java)
            ?: return FetchResult(emptyList(), emptyList())

        val elementType = objectMapper.typeFactory.constructType(typeRef.type).contentType
        val arrayNode = objectMapper.readTree(responseBody)
        if (!arrayNode.isArray) return FetchResult(emptyList(), emptyList())

        val items = mutableListOf<T>()
        val errors = mutableListOf<ParseError>()
        for (node in arrayNode) {
            try {
                @Suppress("UNCHECKED_CAST")
                items.add(objectMapper.treeToValue(node, elementType.rawClass) as T)
            } catch (e: Exception) {
                val recordId = node.get("id")?.takeIf { it.isNumber }?.asLong()
                errors.add(ParseError(recordId, node.toString(), e.message ?: "Unknown error"))
                log.warn("[$endpoint] 레코드 파싱 실패, 건너뜀 (id=$recordId): ${e.message}")
            }
        }
        if (errors.isNotEmpty()) {
            log.warn("[$endpoint] 총 ${errors.size}개 레코드 파싱 실패 (전체 ${arrayNode.size()}개 중)")
        }
        return FetchResult(items, errors)
    }
}
