package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.Table
import java.io.Serializable

@Entity
@Table(name = "game_genre", schema = "service")
class ServiceGameGenreEntity {

    @EmbeddedId
    var id: GameGenreId = GameGenreId()
}

@Embeddable
data class GameGenreId(
    @Column(name = "game_id")
    var gameId: Long = 0L,

    @Column(name = "genre_id")
    var genreId: Long = 0L
) : Serializable
