package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.Table
import java.io.Serializable

@Entity
@Table(name = "game_company", schema = "service")
class ServiceGameCompanyEntity {

    @EmbeddedId
    var id: GameCompanyId = GameCompanyId()

    @Column(name = "is_developer", nullable = false)
    var developer: Boolean = false

    @Column(name = "is_publisher", nullable = false)
    var publisher: Boolean = false

    @Column(name = "is_porting", nullable = false)
    var porting: Boolean = false

    @Column(name = "is_supporting", nullable = false)
    var supporting: Boolean = false
}

@Embeddable
data class GameCompanyId(
    @Column(name = "game_id")
    var gameId: Long = 0L,

    @Column(name = "company_id")
    var companyId: Long = 0L
) : Serializable
