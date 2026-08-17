package inc.uberpopug.accounting.repository

import java.nio.charset.StandardCharsets
import java.sql.SQLException
import java.time.{Instant, LocalDate}
import java.util.UUID

import inc.uberpopug.common.domain.{DomainError, Money, UserId}
import inc.uberpopug.common.domain.DomainError.PersistenceError
import inc.uberpopug.accounting.db.DbContext.Postgres
import inc.uberpopug.accounting.domain.AccountEvent
import inc.uberpopug.accounting.domain.AccountEvent.given
import inc.uberpopug.accounting.domain.{AuditLogEntry, DailyStatsCalculator, PayoutCalculator}
import io.getquill.*
import io.getquill.extras.*
import zio.{ZIO, ZLayer}
import zio.json.*

/** Строка таблицы `events` (event store) в формате БД. */
final case class EventRow(
    id: Long,
    eventId: UUID,
    eventType: String,
    aggregateId: UUID,
    payload: Array[Byte],
    createdAt: Instant
)

/** Строка таблицы `account_balances` в формате БД: инкрементальная проекция баланса. */
final case class BalanceRow(userId: UUID, balanceCents: Long, updatedAt: Instant)

/** Строка таблицы `users` (проекция `UserCreated`) в формате БД. */
final case class UserRow(userId: UUID, name: String, createdAt: Instant)

/** Строка таблицы `processed_events` в формате БД: идемпотентность обработки событий. */
final case class ProcessedEventRow(eventId: UUID, eventType: String, processedAt: Instant)

/** Event store Accounting Service: append-only поток событий с инкрементальной проекцией баланса. Все записи —
  * атомарные транзакции (processed_events dedup + insert events + upsert баланса), поэтому повторная обработка одного
  * Kafka-события идемпотентна.
  */
trait EventStore:
  /** Атомарно: dedup по `dedupKey` (id исходного Kafka-события) + append событий + upsert баланса. `false` — событие
    * уже обработано.
    */
  def append(dedupKey: UUID, events: List[AccountEvent]): ZIO[Any, DomainError, Boolean]

  /** То же, что `append`, плюс запись событий в outbox в той же транзакции (для выплат: `AccountPayout` +
    * `PaymentProcessed`).
    */
  def appendWithOutbox(
      dedupKey: UUID,
      events: List[AccountEvent],
      outbox: List[OutboxRecord]
  ): ZIO[Any, DomainError, Boolean]

  /** Атомарно: dedup по `dedupKey` + проекция пользователя (`users`) + создание счёта с нулевым балансом. `false` —
    * событие уже обработано.
    */
  def upsertUserFromCreated(
      dedupKey: UUID,
      userId: UserId,
      name: String,
      at: Instant
  ): ZIO[Any, DomainError, Boolean]

  /** Текущий баланс счёта; отсутствие счёта — `None`. */
  def balanceOf(userId: UserId): ZIO[Any, DomainError, Option[Money]]

  /** Страница аудитлога счёта (финансовые операции, новее — первыми) и общее число записей. */
  def auditLog(userId: UserId, limit: Int, offset: Int): ZIO[Any, DomainError, (List[AuditLogEntry], Long)]

  /** Все события с `createdAt` в полуинтервале `[from, to)`. */
  def eventsBetween(from: Instant, to: Instant): ZIO[Any, DomainError, List[AccountEvent]]

  /** Все события с `createdAt` строго до `cutoff` (срез для выплат: события после среза — на следующий день). */
  def eventsBefore(cutoff: Instant): ZIO[Any, DomainError, List[AccountEvent]]

  /** Была ли уже выплата пользователю за указанную дату (по детерминированному id события). */
  def payoutExists(userId: UserId, date: LocalDate): ZIO[Any, DomainError, Boolean]

  /** Имя пользователя из проекции; отсутствие — `None`. */
  def findUser(userId: UserId): ZIO[Any, DomainError, Option[String]]

  /** Все пользователи проекции (для выплат и ежедневной статистики). */
  def listUsers: ZIO[Any, DomainError, List[DailyStatsCalculator.UserSnapshot]]

object EventStore:
  /** Слой репозитория поверх Quill-контекста Postgres. */
  val layer: ZLayer[Postgres, Nothing, EventStore] =
    ZLayer.fromFunction(EventStoreLive(_))

/** Quill-реализация event store поверх Postgres. */
final case class EventStoreLive(ctx: Postgres) extends EventStore:
  import ctx.*

  /** Типы финансовых событий, попадающих в аудитлог. */
  private val financialEventTypes = List("AccountDebited", "AccountCredited", "AccountPayout")

  /** Оборачивает SQL-ошибку в `PersistenceError`. */
  private def toPersistenceError(ex: Throwable): DomainError =
    ex match
      case e: SQLException => PersistenceError(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
      case other           => PersistenceError(Option(other.getMessage).getOrElse(other.getClass.getSimpleName))

  /** Сериализует событие в payload (JSON, внутренний формат event store). */
  private def encode(event: AccountEvent): Array[Byte] = event.toJson.getBytes(StandardCharsets.UTF_8)

  /** Десериализует payload; повреждённая строка — `PersistenceError`. */
  private def decode(row: EventRow): ZIO[Any, DomainError, AccountEvent] =
    ZIO.fromEither(
      String(row.payload, StandardCharsets.UTF_8)
        .fromJson[AccountEvent]
        .left
        .map(message => PersistenceError(s"Corrupted event row ${row.eventId}: $message"))
    )

  /** Вставляет id события в `processed_events`; дубликат (SQLState 23505) — `false`. */
  private def insertProcessedIfAbsent(eventId: UUID, eventType: String, at: Instant): ZIO[Any, Throwable, Boolean] =
    run(query[ProcessedEventRow].insertValue(lift(ProcessedEventRow(eventId, eventType, at))))
      .as(true)
      .catchSome { case e: SQLException if e.getSQLState == "23505" => ZIO.succeed(false) }

  /** Вставляет события в event store (без `id` и `metadata`: id генерирует `BIGSERIAL`). */
  private def insertEvents(events: List[AccountEvent]): ZIO[Any, Throwable, Unit] =
    ZIO.foreachDiscard(events) { event =>
      run(
        query[EventRow].insert(
          _.eventId -> lift(event.eventId),
          _.eventType -> lift(event.eventType),
          _.aggregateId -> lift(event.aggregateId),
          _.payload -> lift(encode(event)),
          _.createdAt -> lift(event.timestamp)
        )
      )
    }

  /** Upsert баланса: вставляет строку или прибавляет сумму `deltaCents` всех событий агрегата. */
  private def upsertBalances(events: List[AccountEvent]): ZIO[Any, Throwable, Unit] =
    ZIO.foreachDiscard(events.groupBy(_.aggregateId).toList) { case (userId, userEvents) =>
      val deltaCents = userEvents.map(_.deltaCents).sum
      val updatedAt = userEvents.map(_.timestamp).max
      run(
        query[BalanceRow]
          .insertValue(lift(BalanceRow(userId, deltaCents, updatedAt)))
          .onConflictUpdate(_.userId)(
            (t, e) => t.balanceCents -> (t.balanceCents + e.balanceCents),
            (t, e) => t.updatedAt -> e.updatedAt
          )
      )
    }

  /** Вставляет записи outbox (исключая `id` — генерирует `BIGSERIAL`, см. `insertMeta` в `OutboxRow`). */
  private def insertOutbox(records: List[OutboxRecord]): ZIO[Any, Throwable, Unit] =
    ZIO.foreachDiscard(records)(record => run(query[OutboxRow].insertValue(lift(OutboxRow.fromRecord(record)))).unit)

  def append(dedupKey: UUID, events: List[AccountEvent]): ZIO[Any, DomainError, Boolean] =
    ctx
      .transaction {
        for
          deduplicated <- insertProcessedIfAbsent(dedupKey, events.head.eventType, events.head.timestamp)
          applied <-
            if !deduplicated then ZIO.succeed(false)
            else insertEvents(events) *> upsertBalances(events).as(true)
        yield applied
      }
      .mapError(toPersistenceError)

  def appendWithOutbox(
      dedupKey: UUID,
      events: List[AccountEvent],
      outbox: List[OutboxRecord]
  ): ZIO[Any, DomainError, Boolean] =
    ctx
      .transaction {
        for
          deduplicated <- insertProcessedIfAbsent(dedupKey, events.head.eventType, events.head.timestamp)
          applied <-
            if !deduplicated then ZIO.succeed(false)
            else insertEvents(events) *> upsertBalances(events) *> insertOutbox(outbox).as(true)
        yield applied
      }
      .mapError(toPersistenceError)

  def upsertUserFromCreated(
      dedupKey: UUID,
      userId: UserId,
      name: String,
      at: Instant
  ): ZIO[Any, DomainError, Boolean] =
    ctx
      .transaction {
        for
          deduplicated <- insertProcessedIfAbsent(dedupKey, "UserCreated", at)
          applied <-
            if !deduplicated then ZIO.succeed(false)
            else
              run(query[UserRow].insertValue(lift(UserRow(userId.value, name, at))).onConflictIgnore).unit *>
                run(query[BalanceRow].insertValue(lift(BalanceRow(userId.value, 0L, at))).onConflictIgnore).unit
                  .as(true)
        yield applied
      }
      .mapError(toPersistenceError)

  def balanceOf(userId: UserId): ZIO[Any, DomainError, Option[Money]] =
    run(query[BalanceRow].filter(_.userId == lift(userId.value)))
      .map(_.headOption.map(row => Money.fromCents(row.balanceCents)))
      .mapError(toPersistenceError)

  def auditLog(userId: UserId, limit: Int, offset: Int): ZIO[Any, DomainError, (List[AuditLogEntry], Long)] =
    for
      rows <- run(
        query[EventRow]
          .filter(row =>
            row.aggregateId == lift(userId.value) && liftQuery(financialEventTypes).contains(row.eventType)
          )
          .sortBy(_.createdAt)(using Ord.desc)
          .drop(lift(offset))
          .take(lift(limit))
      ).mapError(toPersistenceError)
      total <- run(
        query[EventRow]
          .filter(row =>
            row.aggregateId == lift(userId.value) && liftQuery(financialEventTypes).contains(row.eventType)
          )
          .size
      ).mapError(toPersistenceError)
      entries <- ZIO.foreach(rows)(row => decode(row).map(AuditLogEntry.fromEvent))
    yield (entries.flatten, total)

  def eventsBetween(from: Instant, to: Instant): ZIO[Any, DomainError, List[AccountEvent]] =
    run(
      query[EventRow]
        .filter(row => row.createdAt >= lift(from) && row.createdAt < lift(to))
        .sortBy(_.createdAt)(using Ord.asc)
    )
      .mapError(toPersistenceError)
      .flatMap(rows => ZIO.foreach(rows)(decode))

  def eventsBefore(cutoff: Instant): ZIO[Any, DomainError, List[AccountEvent]] =
    run(query[EventRow].filter(row => row.createdAt < lift(cutoff)).sortBy(_.createdAt)(using Ord.asc))
      .mapError(toPersistenceError)
      .flatMap(rows => ZIO.foreach(rows)(decode))

  def payoutExists(userId: UserId, date: LocalDate): ZIO[Any, DomainError, Boolean] =
    run(query[EventRow].filter(_.eventId == lift(PayoutCalculator.payoutEventId(userId, date))).nonEmpty)
      .mapError(toPersistenceError)

  def findUser(userId: UserId): ZIO[Any, DomainError, Option[String]] =
    run(query[UserRow].filter(_.userId == lift(userId.value)))
      .map(_.headOption.map(_.name))
      .mapError(toPersistenceError)

  def listUsers: ZIO[Any, DomainError, List[DailyStatsCalculator.UserSnapshot]] =
    run(query[UserRow].sortBy(_.createdAt)(using Ord.asc))
      .map(_.map(row => DailyStatsCalculator.UserSnapshot(row.userId, row.name, row.createdAt)))
      .mapError(toPersistenceError)
