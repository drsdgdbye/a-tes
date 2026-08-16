package inc.uberpopug.taskservice.repository

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.PersistenceError
import inc.uberpopug.taskservice.db.DbContext.Postgres
import io.getquill.*
import zio.{ZIO, ZLayer}

/** Строка таблицы `processed_events` в формате БД: идемпотентность обработки событий. */
final case class ProcessedEventRow(
    eventId: UUID,
    eventType: String,
    processedAt: Instant
)

/** Репозиторий обработанных событий: гарантирует at-least-once обработку без дублей. */
trait ProcessedEventsRepository:
  /** Вставляет событие, если его ещё нет. `false` — событие уже обработано ранее. */
  def insertIfAbsent(eventId: UUID, eventType: String, processedAt: Instant): ZIO[Any, DomainError, Boolean]

object ProcessedEventsRepository:
  /** Слой репозитория поверх Quill-контекста Postgres. */
  val layer: ZLayer[Postgres, Nothing, ProcessedEventsRepository] =
    ZLayer.fromFunction(ProcessedEventsRepositoryLive(_))

/** Quill-реализация репозитория обработанных событий. */
final case class ProcessedEventsRepositoryLive(ctx: Postgres) extends ProcessedEventsRepository:
  import ctx.*

  /** Вставляет событие; нарушение уникальности PK (`23505`) — уже обработано. */
  def insertIfAbsent(eventId: UUID, eventType: String, processedAt: Instant): ZIO[Any, DomainError, Boolean] =
    run(
      query[ProcessedEventRow].insertValue(lift(ProcessedEventRow(eventId, eventType, processedAt)))
    ).as(true)
      .catchSome { case e: SQLException if e.getSQLState == "23505" => ZIO.succeed(false) }
      .mapError(ex => PersistenceError(Option(ex.getMessage).getOrElse(ex.getClass.getSimpleName)))
