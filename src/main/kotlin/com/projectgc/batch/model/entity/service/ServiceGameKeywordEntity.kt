package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.Table
import java.io.Serializable

@Entity
@Table(name = "game_keyword", schema = "service")
class ServiceGameKeywordEntity {

    @EmbeddedId
    var id: GameKeywordId = GameKeywordId()
}

@Embeddable
data class GameKeywordId(
    @Column(name = "game_id")
    var gameId: Long = 0L,

    @Column(name = "keyword_id")
    var keywordId: Long = 0L
) : Serializable
