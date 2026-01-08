package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.Table
import java.io.Serializable

@Entity
@Table(name = "game_theme", schema = "service")
class ServiceGameThemeEntity {

    @EmbeddedId
    var id: GameThemeId = GameThemeId()
}

@Embeddable
data class GameThemeId(
    @Column(name = "game_id")
    var gameId: Long = 0L,

    @Column(name = "theme_id")
    var themeId: Long = 0L
) : Serializable
