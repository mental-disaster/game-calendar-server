package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.Table
import java.io.Serializable

@Entity
@Table(name = "game_game_mode", schema = "service")
class ServiceGameGameModeEntity {

    @EmbeddedId
    var id: GameGameModeId = GameGameModeId()
}

@Embeddable
data class GameGameModeId(
    @Column(name = "game_id")
    var gameId: Long = 0L,

    @Column(name = "game_mode_id")
    var gameModeId: Long = 0L
) : Serializable
