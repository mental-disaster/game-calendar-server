package com.projectgc.batch.client

import com.fasterxml.jackson.annotation.JsonProperty
import com.projectgc.batch.config.IgdbProperties
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import org.springframework.web.util.UriComponentsBuilder
import java.time.Instant

// 현재: 단일 서버 기준 메모리 캐시 + @Synchronized 로 스레드 안전 보장
// 이중화 시: Redis 분산 캐시로 교체 필요 (각 인스턴스가 독립적으로 토큰을 발급하는 문제)
@Component
class TwitchAuthClient(
    private val igdbProperties: IgdbProperties,
    restClientBuilder: RestClient.Builder,  // 테스트 시 MockRestServiceServer 주입을 위해 Builder 주입
) {
    private val restClient = restClientBuilder.build()

    private var cachedToken: String? = null
    private var tokenExpiresAt: Instant = Instant.EPOCH  // MIN 대신 EPOCH — MIN.minusSeconds() 오버플로우 방지

    // 만료 60초 전에 미리 재발급
    private val expiryBuffer = 60L

    @Synchronized
    fun invalidateToken() {
        cachedToken = null
        tokenExpiresAt = Instant.EPOCH
    }

    @Synchronized
    fun getAccessToken(): String =
        cachedToken?.takeIf { tokenExpiresAt.minusSeconds(expiryBuffer).isAfter(Instant.now()) }
            ?: fetchNewToken()

    private fun fetchNewToken(): String {
        val uri = UriComponentsBuilder.fromUriString(igdbProperties.authUrl)
            .queryParam("client_id", igdbProperties.clientId)
            .queryParam("client_secret", igdbProperties.clientSecret)
            .queryParam("grant_type", "client_credentials")
            .build().toUri()

        val response = restClient.post()
            .uri(uri)
            .retrieve()
            .body(TwitchTokenResponse::class.java)
            ?: error("Twitch 토큰 응답이 null입니다")

        cachedToken = response.accessToken
        tokenExpiresAt = Instant.now().plusSeconds(response.expiresIn)

        return response.accessToken
    }

    private data class TwitchTokenResponse(
        @JsonProperty("access_token") val accessToken: String,
        @JsonProperty("expires_in") val expiresIn: Long,
    )
}
