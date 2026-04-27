package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table

/**
 * service.game_localization 테이블 매핑 엔티티입니다.
 */
@Entity
@Table(name = "game_localization", schema = "service")
class ServiceGameLocalizationEntity : ServiceEntity() {

    @Column(name = "game_id", nullable = false)
    var gameId: Long = 0L

    @Column(name = "region_id")
    var regionId: Long? = null

    @Column(name = "name", nullable = false)
    var localizedName: String = ""
}
