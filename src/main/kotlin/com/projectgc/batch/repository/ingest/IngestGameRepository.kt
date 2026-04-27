package com.projectgc.batch.repository.ingest

import com.projectgc.batch.models.entity.ingest.IngestGameEntity
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Slice
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query

interface IngestGameRepository : JpaRepository<IngestGameEntity, Long> {

    // 전체 미디어 초기 수집용 — Keyset Pagination
    @Query("SELECT g.id FROM IngestGameEntity g WHERE g.id > :lastId ORDER BY g.id ASC")
    fun findAllIdsAfter(lastId: Long, pageable: Pageable): Slice<Long>

    // 증분 미디어 동기화용 — 미디어 커서 이후 변경된 게임만 반환
    // game.updatedAt이 갱신되면 자식 미디어도 변경된 것으로 간주
    @Query("SELECT g.id FROM IngestGameEntity g WHERE g.updatedAt > :updatedAfter AND g.id > :lastId ORDER BY g.id ASC")
    fun findAllIdsUpdatedAfter(lastId: Long, updatedAfter: Long, pageable: Pageable): Slice<Long>
}
