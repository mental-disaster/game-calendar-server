package com.projectgc.batch.service

import com.projectgc.batch.client.FetchResult
import com.projectgc.batch.client.IgdbClient
import com.projectgc.batch.models.entity.ingest.IngestSyncCursorEntity
import com.projectgc.batch.repository.ingest.IngestGameRepository
import com.projectgc.batch.repository.ingest.IngestInvolvedCompanyRepository
import com.projectgc.batch.repository.ingest.IngestJdbcRepository
import com.projectgc.batch.repository.ingest.IngestRepositories
import com.projectgc.batch.repository.ingest.IngestSyncCursorRepository
import com.projectgc.shared.event.IngestSyncSucceededEvent
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.context.ApplicationEventPublisher
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.SliceImpl
import java.time.Instant
import java.util.Optional
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class GameReleaseBatchServiceTest {
    private val igdbClient = mock(IgdbClient::class.java)
    private val mediaSyncService = mock(MediaSyncService::class.java)
    private val repos = mock(IngestRepositories::class.java)
    private val jdbc = mock(IngestJdbcRepository::class.java)
    private val syncCursorRepository = mock(IngestSyncCursorRepository::class.java)
    private val gameRepository = mock(IngestGameRepository::class.java)
    private val involvedCompanyRepository = mock(IngestInvolvedCompanyRepository::class.java)
    private val eventPublisher = RecordingEventPublisher()
    private val savedCursors = mutableListOf<IngestSyncCursorEntity>()

    private val service = GameReleaseBatchService(
        igdbClient = igdbClient,
        mediaSync = mediaSyncService,
        repos = repos,
        eventPublisher = eventPublisher,
    )

    @BeforeEach
    fun setUp() {
        `when`(repos.jdbc).thenReturn(jdbc)
        `when`(repos.syncCursor).thenReturn(syncCursorRepository)
        `when`(repos.game).thenReturn(gameRepository)
        `when`(repos.involvedCompany).thenReturn(involvedCompanyRepository)

        `when`(syncCursorRepository.findById(org.mockito.Mockito.anyString())).thenReturn(Optional.empty())
        doAnswer { invocation ->
            (invocation.arguments[0] as IngestSyncCursorEntity).also { savedCursors += it }
        }
            .`when`(syncCursorRepository).save(anyObject(IngestSyncCursorEntity::class.java))

        `when`(gameRepository.findAllIdsAfter(org.mockito.ArgumentMatchers.anyLong(), anyObject(Pageable::class.java)))
            .thenReturn(SliceImpl(emptyList()))
        `when`(involvedCompanyRepository.findAllDistinctCompanyIds()).thenReturn(emptyList())

        stubSuccessfulFetches()
    }

    @Test
    fun `publishes shared ingest success event only after authoritative success`() {
        service.syncAll()

        assertEquals(1, eventPublisher.events.size)
        val event = eventPublisher.events.single() as IngestSyncSucceededEvent
        assertTrue(event.completedAt <= Instant.now())

        verify(jdbc).finishSyncLog(anyObject(UUID::class.java), anyObject(Instant::class.java), eqValue("completed"))
        verify(jdbc, never()).finishSyncLog(anyObject(UUID::class.java), anyObject(Instant::class.java), eqValue("failed"))
    }

    @Test
    fun `does not publish downstream when a source step partially fails`() {
        `when`(igdbClient.fetchGenres()).thenThrow(RuntimeException("genre fetch failed"))

        service.syncAll()

        assertTrue(eventPublisher.events.isEmpty())
        verify(jdbc, never()).finishSyncLog(anyObject(UUID::class.java), anyObject(Instant::class.java), eqValue("completed"))
        verify(jdbc).finishSyncLog(anyObject(UUID::class.java), anyObject(Instant::class.java), eqValue("failed"))
    }

    @Test
    fun `does not publish downstream when sync setup fails before completion`() {
        doThrow(RuntimeException("sync log insert failed"))
            .`when`(jdbc).insertSyncLog(anyObject(UUID::class.java), anyObject(Instant::class.java))

        service.syncAll()

        assertTrue(eventPublisher.events.isEmpty())
        verify(jdbc, never()).finishSyncLog(anyObject(UUID::class.java), anyObject(Instant::class.java), eqValue("completed"))
        verify(jdbc, never()).finishSyncLog(anyObject(UUID::class.java), anyObject(Instant::class.java), eqValue("failed"))
    }

    @Test
    fun `does not publish downstream when completed finish log write fails`() {
        doThrow(RuntimeException("completed log write failed"))
            .`when`(jdbc).finishSyncLog(anyObject(UUID::class.java), anyObject(Instant::class.java), eqValue("completed"))

        service.syncAll()

        assertTrue(eventPublisher.events.isEmpty())
        verify(jdbc).finishSyncLog(anyObject(UUID::class.java), anyObject(Instant::class.java), eqValue("failed"))
    }

    @Test
    fun `does not publish downstream and does not advance cursor when loop guard is exceeded`() {
        val loopingSlice = SliceImpl(listOf(1L), Pageable.ofSize(1), true)
        val emptySlice = SliceImpl<Long>(emptyList())
        val calls = AtomicInteger(0)
        `when`(gameRepository.findAllIdsAfter(org.mockito.ArgumentMatchers.anyLong(), anyObject(Pageable::class.java)))
            .thenAnswer {
                if (calls.incrementAndGet() <= 10_000) loopingSlice else emptySlice
            }

        service.syncAll()

        assertTrue(eventPublisher.events.isEmpty())
        assertTrue(savedCursors.none { it.tableName == "cover" })
        verify(jdbc, never()).finishSyncLog(anyObject(UUID::class.java), anyObject(Instant::class.java), eqValue("completed"))
        verify(jdbc).finishSyncLog(anyObject(UUID::class.java), anyObject(Instant::class.java), eqValue("failed"))
    }

    private fun stubSuccessfulFetches() {
        `when`(igdbClient.fetchGenres()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchThemes()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchPlayerPerspectives()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchGameModes()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchLanguageSupportTypes()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchPlatformTypes()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchWebsiteTypes()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchGameStatuses()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchGameTypes()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchLanguages()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchRegions()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchReleaseDateRegions()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchReleaseDateStatuses()).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchGames(0L, 0L)).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchReleaseDates(0L, 0L)).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchPlatforms(0L, 0L)).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchInvolvedCompanies(0L, 0L)).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchLanguageSupports(0L, 0L)).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchGameLocalizations(0L, 0L)).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchKeywords(0L)).thenReturn(emptyFetchResult())
        `when`(igdbClient.fetchPlatformLogos(0L)).thenReturn(emptyFetchResult())
    }

    private fun <T> emptyFetchResult(): FetchResult<T> = FetchResult(
        items = emptyList(),
        errors = emptyList(),
    )

    @Suppress("UNCHECKED_CAST")
    private fun <T> anyObject(type: Class<T>): T {
        org.mockito.Mockito.any(type)
        return null as T
    }

    private fun <T> eqValue(value: T): T {
        org.mockito.ArgumentMatchers.eq(value)
        return value
    }

    private class RecordingEventPublisher : ApplicationEventPublisher {
        val events = mutableListOf<Any>()

        override fun publishEvent(event: Any) {
            events += event
        }
    }
}
