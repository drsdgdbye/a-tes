package inc.uberpopug.accounting

import java.time.{Instant, LocalDate}
import java.util.UUID

import zio.{Chunk, Ref, ZIO}

import inc.uberpopug.accounting.domain.{
  AccountEvent,
  AuditLogEntry,
  BalanceCalculator,
  DailyStatsCalculator,
  PayoutCalculator
}
import inc.uberpopug.accounting.repository.{EventStore, OutboxRecord}
import inc.uberpopug.common.domain.{DomainError, Money, UserId}

/** Упрощает `.orDie` для типизированных ошибок `DomainError` (не `Throwable`): превращает ошибку в исключение и роняет
  * эффект — «этого не должно случиться в тесте».
  */
extension [R, A](io: ZIO[R, DomainError, A])
  def orDieE: ZIO[R, Nothing, A] = io.mapError(error => new RuntimeException(error.toString)).orDie

/** In-memory состояние event store для тестов: события, проекция пользователей, дедупликация, outbox. */
final case class InMemoryEventStoreState(
    events: List[AccountEvent] = Nil,
    users: Map[UUID, DailyStatsCalculator.UserSnapshot] = Map.empty,
    processed: Set[UUID] = Set.empty,
    outbox: Chunk[OutboxRecord] = Chunk.empty
)

/** In-memory реализация `EventStore` на Ref с той же семантикой, что и Quill-реализация: dedup по `dedupKey`,
  * атомарность записи, баланс как проекция событий.
  */
final case class InMemoryEventStore(state: Ref[InMemoryEventStoreState]) extends EventStore:
  /** Типы финансовых событий, попадающих в аудитлог. */
  private val financialEventTypes = List("AccountDebited", "AccountCredited", "AccountPayout")

  def append(dedupKey: UUID, events: List[AccountEvent]): ZIO[Any, DomainError, Boolean] =
    state.modify { s =>
      if s.processed.contains(dedupKey) then (false, s)
      else (true, s.copy(events = s.events ++ events, processed = s.processed + dedupKey))
    }

  def appendWithOutbox(
      dedupKey: UUID,
      events: List[AccountEvent],
      outbox: List[OutboxRecord]
  ): ZIO[Any, DomainError, Boolean] =
    state.modify { s =>
      if s.processed.contains(dedupKey) then (false, s)
      else (true, s.copy(events = s.events ++ events, processed = s.processed + dedupKey, outbox = s.outbox ++ outbox))
    }

  def upsertUserFromCreated(
      dedupKey: UUID,
      userId: UserId,
      name: String,
      at: Instant
  ): ZIO[Any, DomainError, Boolean] =
    state.modify { s =>
      if s.processed.contains(dedupKey) then (false, s)
      else
        (
          true,
          s.copy(
            events = s.events,
            users = s.users + (userId.value -> DailyStatsCalculator.UserSnapshot(userId.value, name, at)),
            processed = s.processed + dedupKey
          )
        )
    }

  def balanceOf(userId: UserId): ZIO[Any, DomainError, Option[Money]] =
    state.get.map { s =>
      val uid = userId.value
      val exists = s.users.contains(uid) || s.events.exists(_.aggregateId == uid)
      if exists then Some(BalanceCalculator.currentBalance(s.events.filter(_.aggregateId == uid)))
      else None
    }

  def auditLog(userId: UserId, limit: Int, offset: Int): ZIO[Any, DomainError, (List[AuditLogEntry], Long)] =
    state.get.map { s =>
      val relevant = s.events
        .filter(e => e.aggregateId == userId.value && financialEventTypes.contains(e.eventType))
        .sortBy(_.timestamp)(using Ordering[Instant].reverse)
      (relevant.slice(offset, offset + limit).flatMap(AuditLogEntry.fromEvent), relevant.size.toLong)
    }

  def eventsBetween(from: Instant, to: Instant): ZIO[Any, DomainError, List[AccountEvent]] =
    state.get.map(
      _.events.filter(e => !e.timestamp.isBefore(from) && e.timestamp.isBefore(to)).sortBy(_.timestamp)
    )

  def eventsBefore(cutoff: Instant): ZIO[Any, DomainError, List[AccountEvent]] =
    state.get.map(_.events.filter(_.timestamp.isBefore(cutoff)).sortBy(_.timestamp))

  def payoutExists(userId: UserId, date: LocalDate): ZIO[Any, DomainError, Boolean] =
    state.get.map(_.events.exists(_.eventId == PayoutCalculator.payoutEventId(userId, date)))

  def findUser(userId: UserId): ZIO[Any, DomainError, Option[String]] =
    state.get.map(_.users.get(userId.value).map(_.name))

  def listUsers: ZIO[Any, DomainError, List[DailyStatsCalculator.UserSnapshot]] =
    state.get.map(_.users.values.toList.sortBy(_.createdAt))
