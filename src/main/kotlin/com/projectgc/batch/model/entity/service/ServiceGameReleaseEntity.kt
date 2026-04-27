package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Table
import java.time.Instant

@Entity
@Table(name = "game_release", schema = "service")
class ServiceGameReleaseEntity : ServiceEntity() {

    @Column(name = "game_id", nullable = false)
    var gameId: Long = 0L

    @Column(name = "platform_id")
    var platformId: Long? = null

    @Column(name = "region_id")
    var regionId: Long? = null

    @Column(name = "status_id")
    var statusId: Long? = null

    @Column(name = "release_date")
    var releaseDate: Instant? = null

    @Column(name = "year")
    var releaseYear: Int? = null

    @Column(name = "month")
    var releaseMonth: Int? = null

    @Column(name = "date_human")
    var humanReadableDate: String? = null
}
