package inc.uberpopug.auth.service

import java.time.Instant
import java.util.UUID

import zio.{Ref, ZIO}
import zio.test.*

import inc.uberpopug.auth.config.{
  AppConfig,
  AuthConfig,
  DatabaseConfig,
  JwtConfig,
  KafkaConfig,
  OutboxConfig,
  ServerConfig
}
import inc.uberpopug.auth.repository.{OutboxRecord, OutboxRepository, OutboxRow}

/** In-memory OutboxRepository для тестов relay: записи и флаг published на Ref. */
final case class OutboxRepoInMemory(state: Ref[List[OutboxRow]]) extends OutboxRepository:
  private var nextId = 0L

  def insert(record: OutboxRecord): ZIO[Any, Nothing, Unit] =
    ZIO.succeed {
      nextId += 1
      ()
    } *> state.update(
      _ :+ OutboxRow(nextId, record.aggregateId, record.eventType, record.payload, record.createdAt, published = false)
    )

  def claimBatch(limit: Int): ZIO[Any, Nothing, List[OutboxRow]] =
    state.get.map(_.filterNot(_.published).sortBy(_.id).take(limit))

  def markPublished(ids: List[Long]): ZIO[Any, Nothing, Unit] =
    state.update(_.map(r => if ids.contains(r.id) then r.copy(published = true) else r))

/** In-memory OutboxPublisher: перехватывает (key, payload); может быть настроен на сбой. */
final case class PublisherInMemory(captured: Ref[List[(String, Array[Byte])]], failNext: Ref[Boolean])
    extends OutboxPublisher:
  def publish(key: String, value: Array[Byte]): ZIO[Any, Throwable, Unit] =
    for
      fail <- failNext.getAndSet(false)
      _ <- if fail then ZIO.fail(new RuntimeException("publish failed")) else ZIO.unit
      _ <- captured.update(_ :+ (key, value))
    yield ()

object OutboxRelaySpec extends ZIOSpecDefault:
  private val cfg = AppConfig(
    server = ServerConfig(10001),
    database = DatabaseConfig("jdbc:postgresql://localhost:5432/ates_auth", "ates", "ates", 10),
    kafka = KafkaConfig(List("localhost:29092"), "auth.user.created"),
    jwt = JwtConfig("ates-auth", 900, 604800),
    outbox = OutboxConfig(batchSize = 50, pollIntervalSeconds = 2),
    auth = AuthConfig(registrationEnabled = true)
  )

  private def makeRelay(
      repo: OutboxRepository,
      publisher: OutboxPublisher
  ): OutboxRelayLive =
    OutboxRelayLive(repo, publisher, cfg)

  private def seedRecord(createdAt: Instant = Instant.parse("2026-01-01T00:00:00Z")): OutboxRecord =
    OutboxRecord(
      aggregateId = UUID.randomUUID(),
      eventType = "UserCreated",
      payload = Array[Byte](1, 2, 3),
      createdAt = createdAt
    )

  def spec: Spec[Any, Any] =
    suite("OutboxRelay")(
      test("processBatch publishes a pending record and marks it published (M-AUTH-30)") {
        for
          state <- Ref.make(List.empty[OutboxRow])
          captured <- Ref.make(List.empty[(String, Array[Byte])])
          fail <- Ref.make(false)
          repo = OutboxRepoInMemory(state)
          publisher = PublisherInMemory(captured, fail)
          record = seedRecord()
          _ <- repo.insert(record)
          relay = makeRelay(repo, publisher)
          _ <- relay.processBatch
          rows <- state.get
          published <- captured.get
          rowsAfter <- relay.processBatch *> state.get
          publishedAfter <- captured.get
        yield assertTrue(
          published.headOption.exists(_._1 == record.aggregateId.toString),
          published.headOption.exists(_._2.sameElements(record.payload)),
          rows.head.published,
          publishedAfter.size == 1
        ) && assertTrue(rowsAfter.forall(_.published))
      },
      test("processBatch leaves the record unpublished on a publish failure (M-AUTH-30)") {
        for
          state <- Ref.make(List.empty[OutboxRow])
          captured <- Ref.make(List.empty[(String, Array[Byte])])
          fail <- Ref.make(true)
          repo = OutboxRepoInMemory(state)
          publisher = PublisherInMemory(captured, fail)
          record = seedRecord()
          _ <- repo.insert(record)
          relay = makeRelay(repo, publisher)
          _ <- relay.processBatch
          rows <- state.get
          published <- captured.get
          _ <- relay.processBatch
          publishedAfter <- captured.get
        yield assertTrue(
          !rows.head.published,
          published.isEmpty,
          publishedAfter.size == 1
        )
      }
    )
