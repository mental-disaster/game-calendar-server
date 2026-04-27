package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.Table
import java.io.Serializable

@Entity
@Table(name = "game_player_perspective", schema = "service")
class ServiceGamePlayerPerspectiveEntity {

    @EmbeddedId
    var id: GamePlayerPerspectiveId = GamePlayerPerspectiveId()
}

@Embeddable
data class GamePlayerPerspectiveId(
    @Column(name = "game_id")
    var gameId: Long = 0L,

    @Column(name = "player_perspective_id")
    var playerPerspectiveId: Long = 0L
) : Serializable
