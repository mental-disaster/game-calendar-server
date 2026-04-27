package com.projectgc.batch.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.bind.DefaultValue

@ConfigurationProperties(prefix = "igdb")
data class IgdbProperties(
    val clientId: String,
    val clientSecret: String,
    val authUrl: String,
    val baseUrl: String,
    val filter: Filter,
) {
    data class Filter(
        // 수집 대상 game_type ID 목록
        // 0=Main Game, 1=DLC/Addon, 2=Expansion, 4=Standalone Expansion,
        // 6=Episode, 7=Season, 8=Remake, 9=Remaster
        val gameTypes: List<Int>,

        // 수집 기준 최초 출시일 하한 (IGDB Unix timestamp)
        @DefaultValue("0")
        val minFirstReleaseDate: Long,

        // 수집 대상 플랫폼 ID 목록 (비어 있으면 전체 플랫폼)
        // 6=PC(Windows), 14=macOS, 34=Android, 39=iOS,
        // 48=PS4, 130=Nintendo Switch, 167=PS5, 169=Xbox Series X|S, 508=Nintendo Switch 2
        val platformIds: List<Int> = emptyList(),
    )
}
