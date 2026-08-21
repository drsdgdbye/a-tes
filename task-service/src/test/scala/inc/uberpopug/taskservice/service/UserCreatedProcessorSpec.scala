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
import inc.uberpopug.taskservice.domain.Role
import inc.uberpopug.taskservice.repository.{ProcessedEventsRepository, UserRepository}

/** In-memory реализация ProcessedEventsRepository на Ref для тестов дедупликации. */
final case class ProcessedEventsInMemory(processed: Ref[Set[UUID]]) extends ProcessedEventsRepository:
  def insertIfAbsent(eventId: UUID, eventType: String, processedAt: Instant): ZIO[Any, DomainError, Boolean] =
    processed.modify(s => if s.contains(eventId) then (false, s) else (true, s + eventId))

/** In-memory реализация UserRepository на Ref для тестов проекции users. */
final case class UserRepositoryInMemory(users: Ref[List[(UserId, String, Role)]]) extends UserRepository:
  def insertIfAbsent(userId: UserId, name: String, role: Role): ZIO[Any, DomainError, Unit] =
    users.update(list => if list.exists(_._1 == userId) then list else list :+ (userId, name, role))

  def findAllPopugs(): ZIO[Any, DomainError, List[UserId]] =
    users.get.map(_.filter(_._3 == Role.Popug).map(_._1))

/** Тесты обработчика событий `UserCreated`: дедупликация, наполнение кэша и запись проекции. */
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

  private def makeProcessor: ZIO[Any, Nothing, (EligiblePopugs, Ref[Set[UUID]], UserRepositoryInMemory)] =
    for
      cacheRef <- Ref.make(Set.empty[UserId])
      processedRef <- Ref.make(Set.empty[UUID])
      usersRef <- Ref.make(List.empty[(UserId, String, Role)])
    yield (EligiblePopugs(cacheRef), processedRef, UserRepositoryInMemory(usersRef))

  def spec: Spec[Any, Any] =
    suite("UserCreatedProcessor")(
      test("adds a popug to the eligible cache") {
        for
          event = userCreated(UUID.randomUUID().toString, UUID.randomUUID().toString, "popug")
          (cache, processedRef, userRepo) <- makeProcessor
          _ <- UserCreatedProcessor.process(
            event.toByteArray,
            now,
            ProcessedEventsInMemory(processedRef),
            cache,
            userRepo
          )
          popugs <- cache.all
          users <- userRepo.users.get
        yield assertTrue(popugs.size == 1, users.size == 1)
      },
      test("ignores non-popug roles for cache but writes to users table") {
        for
          event = userCreated(UUID.randomUUID().toString, UUID.randomUUID().toString, "manager")
          (cache, processedRef, userRepo) <- makeProcessor
          _ <- UserCreatedProcessor.process(
            event.toByteArray,
            now,
            ProcessedEventsInMemory(processedRef),
            cache,
            userRepo
          )
          popugs <- cache.all
          users <- userRepo.users.get
        yield assertTrue(popugs.isEmpty, users.size == 1)
      },
      test("is idempotent for a duplicated event") {
        for
          eventId = UUID.randomUUID().toString
          event = userCreated(eventId, UUID.randomUUID().toString, "popug")
          (cache, processedRef, userRepo) <- makeProcessor
          repo = ProcessedEventsInMemory(processedRef)
          _ <- UserCreatedProcessor.process(event.toByteArray, now, repo, cache, userRepo)
          _ <- UserCreatedProcessor.process(event.toByteArray, now, repo, cache, userRepo)
          popugs <- cache.all
          count <- processedRef.get.map(_.size)
          users <- userRepo.users.get
        yield assertTrue(popugs.size == 1, count == 1, users.size == 1)
      },
      test("fails with PersistenceError on malformed bytes") {
        for
          (cache, processedRef, userRepo) <- makeProcessor
          result <- UserCreatedProcessor
            .process("not protobuf".getBytes, now, ProcessedEventsInMemory(processedRef), cache, userRepo)
            .exit
        yield assert(result)(fails(isSubtype[PersistenceError](anything)))
      }
    )
