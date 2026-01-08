package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.Table
import java.io.Serializable

@Entity
@Table(name = "game_language", schema = "service")
class ServiceGameLanguageEntity {

    @EmbeddedId
    var id: GameLanguageId = GameLanguageId()

    @Column(name = "supports_audio", nullable = false)
    var supportsAudio: Boolean = false

    @Column(name = "supports_subtitles", nullable = false)
    var supportsSubtitles: Boolean = false

    @Column(name = "supports_interface", nullable = false)
    var supportsInterface: Boolean = false
}

@Embeddable
data class GameLanguageId(
    @Column(name = "game_id")
    var gameId: Long = 0L,

    @Column(name = "language_id")
    var languageId: Long = 0L
) : Serializable
