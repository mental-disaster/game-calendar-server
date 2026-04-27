package com.projectgc.batch.model.entity.service

import jakarta.persistence.Column
import jakarta.persistence.Id
import jakarta.persistence.MappedSuperclass

/**
 * service 스키마의 단일 PK 테이블이 공유하는 기본 엔티티입니다.
 */
@MappedSuperclass
abstract class ServiceEntity(
    @Id
    @Column(name = "id")
    open var id: Long = 0L
)
