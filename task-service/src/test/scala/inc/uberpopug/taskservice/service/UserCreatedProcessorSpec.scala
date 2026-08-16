package inc.uberpopug.taskservice.service

import java.time.Instant
import java.util.UUID

import zio.{Ref, ZIO}
import zio.test.Assertion.*
import zio.test.*

import auth.user_created.UserCreated

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.PersistenceError
import inc.uberpopug.common.domain.UserId
import inc.uberpopug.taskservice.repository.ProcessedEventsRepository

/** In-memory реализация ProcessedEventsRepository на Ref для тестов дедупликации. */
final case class ProcessedEventsInMemory(processed: Ref[Set[UUID]]) extends ProcessedEventsRepository:
  def insertIfAbsent(eventId: UUID, eventType: String, processedAt: Instant): ZIO[Any, DomainError, Boolean] =
    processed.modify(s => if s.contains(eventId) then (false, s) else (true, s + eventId))

/** Тесты обработчика событий `UserCreated`: дедупликация и наполнение кэша попугов. */
object UserCreatedProcessorSpec extends ZIOSpecDefault:
  private val now = Instant.parse("2026-01-01T00:00:00Z")

  private def userCreated(eventId: String, userId: String, role: String): UserCreated =
    UserCreated(
      eventId = eventId,
      timestamp = now.toEpochMilli,
      version = 1,
      userId = userId,
      name = "Popug",
      email = "popug@ates.io",
      role = role
    )

  private def makeProcessor: ZIO[Any, Nothing, (EligiblePopugs, Ref[Set[UUID]])] =
    for
      cacheRef <- Ref.make(Set.empty[UserId])
      processedRef <- Ref.make(Set.empty[UUID])
      _ <- ZIO.unit
    yield (EligiblePopugs(cacheRef), processedRef)

  def spec: Spec[Any, Any] =
    suite("UserCreatedProcessor")(
      test("adds a popug to the eligible cache") {
        for
          event = userCreated(UUID.randomUUID().toString, UUID.randomUUID().toString, "popug")
          (cache, processedRef) <- makeProcessor
          _ <- UserCreatedProcessor.process(event.toByteArray, now, ProcessedEventsInMemory(processedRef), cache)
          popugs <- cache.all
        yield assertTrue(popugs.size == 1)
      },
      test("ignores non-popug roles") {
        for
          event = userCreated(UUID.randomUUID().toString, UUID.randomUUID().toString, "manager")
          (cache, processedRef) <- makeProcessor
          _ <- UserCreatedProcessor.process(event.toByteArray, now, ProcessedEventsInMemory(processedRef), cache)
          popugs <- cache.all
        yield assertTrue(popugs.isEmpty)
      },
      test("is idempotent for a duplicated event") {
        for
          eventId = UUID.randomUUID().toString
          event = userCreated(eventId, UUID.randomUUID().toString, "popug")
          (cache, processedRef) <- makeProcessor
          repo = ProcessedEventsInMemory(processedRef)
          _ <- UserCreatedProcessor.process(event.toByteArray, now, repo, cache)
          _ <- UserCreatedProcessor.process(event.toByteArray, now, repo, cache)
          popugs <- cache.all
          count <- processedRef.get.map(_.size)
        yield assertTrue(popugs.size == 1, count == 1)
      },
      test("fails with PersistenceError on malformed bytes") {
        for
          (cache, processedRef) <- makeProcessor
          result <- UserCreatedProcessor
            .process("not protobuf".getBytes, now, ProcessedEventsInMemory(processedRef), cache)
            .exit
        yield assert(result)(fails(isSubtype[PersistenceError](anything)))
      }
    )
