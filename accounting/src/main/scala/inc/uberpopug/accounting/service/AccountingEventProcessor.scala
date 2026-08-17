package inc.uberpopug.accounting.service

import java.time.Instant
import java.util.UUID

import scala.util.Try

import zio.ZIO

import inc.uberpopug.common.domain.{DomainError, UserId}
import inc.uberpopug.common.domain.DomainError.{AccountNotFound, InvalidValue}
import inc.uberpopug.accounting.repository.EventStore

import auth.user_created.UserCreated
import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted
import task.task_created.TaskCreated

/** Чистая обработка одного Kafka-события: дедупликация через `processed_events`, маппинг в события event store,
  * проверка существования счёта. Вынесена из consumer-а для тестируемости без Kafka.
  */
object AccountingEventProcessor:
  /** `UserCreated` → проекция пользователя + создание счёта с нулевым балансом. */
  def processUserCreated(event: UserCreated, store: EventStore): ZIO[Any, DomainError, Unit] =
    for
      userId <- ZIO.fromEither(UserId.from(event.userId))
      eventId <- parseUuid(event.eventId)
      _ <- store.upsertUserFromCreated(eventId, userId, event.name, Instant.ofEpochMilli(event.timestamp))
    yield ()

  /** `TaskCreated` → `TaskPriceRecorded` (цены задачи). Счёт исполнителя должен существовать. */
  def processTaskCreated(event: TaskCreated, store: EventStore): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      _ <- requireAccount(event.assigneeId, store)
      _ <- store.append(eventId, List(AccountingEventMapper.taskPriceRecorded(event)))
    yield ()

  /** `TaskAssigned` → списание с нового исполнителя и возврат старому (если был). */
  def processTaskAssigned(event: TaskAssigned, store: EventStore): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      accounts = List(event.newAssigneeId) ++ Option.when(event.oldAssigneeId.nonEmpty)(event.oldAssigneeId)
      _ <- ZIO.foreachDiscard(accounts)(userId => requireAccount(userId, store))
      _ <- store.append(eventId, AccountingEventMapper.assignedEvents(event))
    yield ()

  /** `TaskCompleted` → начисление CompleteReward исполнителю. */
  def processTaskCompleted(event: TaskCompleted, store: EventStore): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      _ <- requireAccount(event.assigneeId, store)
      _ <- store.append(eventId, List(AccountingEventMapper.completedReward(event)))
    yield ()

  /** Событие для несуществующего счёта — poison pill (M-ACC-06): в DLQ. */
  private def requireAccount(userId: String, store: EventStore): ZIO[Any, DomainError, Unit] =
    for
      uid <- ZIO.fromEither(UserId.from(userId))
      name <- store.findUser(uid)
      _ <-
        if name.isDefined then ZIO.unit
        else ZIO.fail(AccountNotFound(userId))
    yield ()

  /** Безопасный парсинг UUID: невалидное значение — `InvalidValue`. */
  private def parseUuid(value: String): ZIO[Any, DomainError, UUID] =
    ZIO.fromEither(
      Try(UUID.fromString(value)).toEither.left.map(_ => InvalidValue("event", s"Invalid UUID: '$value'"))
    )
