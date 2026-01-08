package com.projectgc.batch.model.entity.service

import com.projectgc.batch.model.service.GameRelationType
import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Table
import java.io.Serializable

/**
 * service.game_relation 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "game_relation", schema = "service")
class ServiceGameRelationEntity {

    @EmbeddedId
    var id: GameRelationId = GameRelationId()
}

@Embeddable
data class GameRelationId(
    @Column(name = "game_id")
    var gameId: Long = 0L,

    @Column(name = "related_game_id")
    var relatedGameId: Long = 0L,

    @Enumerated(EnumType.STRING)
    @Column(name = "relation_type")
    var relationType: GameRelationType = GameRelationType.PARENT
) : Serializable
