package com.projectgc.calendar.persistence

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.LocalDate

/**
 * JPA entity that maps an individual game release row.
 */
@Entity
@Table(name = "game_release")
open class GameReleaseEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    open val id: Long? = null,

    @Column(nullable = false)
    open val title: String = "",

    @Column(nullable = false)
    open val releaseDate: LocalDate = LocalDate.now(),

    @Column(nullable = true)
    open val platform: String? = null
)
