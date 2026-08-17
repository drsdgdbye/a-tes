package inc.uberpopug.accounting.service

import java.time.Instant
import java.util.UUID

import scala.util.Try

import zio.ZIO

import inc.uberpopug.common.domain.{DomainError, Money, TaskId, UserId}
import inc.uberpopug.common.domain.DomainError.{AccountNotFound, InvalidValue}
import inc.uberpopug.accounting.repository.EventStore

import auth.user_created.UserCreated
import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted
import task.task_created.TaskCreated

/** Чистая обработка одного Kafka-события: дедупликация через `processed_events`, безопасный парсинг примитивов
  * (невалидные значения — `InvalidValue` через typed error channel, без исключений), маппинг в события event store,
  * проверка существования счёта. Вынесена из consumer-а для тестируемости без Kafka.
  */
object AccountingEventProcessor:
  /** `UserCreated` → проекция пользователя + создание счёта с нулевым балансом. */
  def processUserCreated(event: UserCreated, store: EventStore): ZIO[Any, DomainError, Unit] =
    for
      userId <- parseUserId(event.userId)
      eventId <- parseUuid(event.eventId)
      _ <- store.upsertUserFromCreated(eventId, userId, event.name, Instant.ofEpochMilli(event.timestamp))
    yield ()

  /** `TaskCreated` → `TaskPriceRecorded` (цены задачи). Счёт исполнителя должен существовать. */
  def processTaskCreated(event: TaskCreated, store: EventStore): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      taskId <- parseTaskId(event.taskId)
      assigneeId <- parseUserId(event.assigneeId)
      _ <- requireAccount(assigneeId, store)
      _ <- store.append(
        eventId,
        List(
          AccountingEventMapper.taskPriceRecorded(
            timestamp = Instant.ofEpochMilli(event.timestamp),
            taskId = taskId,
            userId = assigneeId,
            assignFee = Money.fromCents(event.assignFeeCents),
            completeReward = Money.fromCents(event.completeRewardCents)
          )
        )
      )
    yield ()

  /** `TaskAssigned` → списание с нового исполнителя и возврат старому (если был). */
  def processTaskAssigned(event: TaskAssigned, store: EventStore): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      taskId <- parseTaskId(event.taskId)
      newAssignee <- parseUserId(event.newAssigneeId)
      oldAssignee <- if event.oldAssigneeId.nonEmpty then parseUserId(event.oldAssigneeId).map(Some(_)) else ZIO.none
      _ <- requireAccount(newAssignee, store)
      _ <- ZIO.foreachDiscard(oldAssignee)(assignee => requireAccount(assignee, store))
      _ <- store.append(
        eventId,
        AccountingEventMapper.assignedEvents(
          timestamp = Instant.ofEpochMilli(event.timestamp),
          taskId = taskId,
          newAssignee = newAssignee,
          oldAssignee = oldAssignee,
          assignFee = Money.fromCents(event.assignFeeCents)
        )
      )
    yield ()

  /** `TaskCompleted` → начисление CompleteReward исполнителю. */
  def processTaskCompleted(event: TaskCompleted, store: EventStore): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      taskId <- parseTaskId(event.taskId)
      assigneeId <- parseUserId(event.assigneeId)
      _ <- requireAccount(assigneeId, store)
      _ <- store.append(
        eventId,
        List(
          AccountingEventMapper.completedReward(
            timestamp = Instant.ofEpochMilli(event.timestamp),
            taskId = taskId,
            userId = assigneeId,
            completeReward = Money.fromCents(event.completeRewardCents)
          )
        )
      )
    yield ()

  /** Событие для ещё не существующего счёта — транзиентная ошибка (M-ACC-06): при восстановлении `auth.user.created`
    * приходит позже, поток прерывается и событие переобрабатывается после переподписки (нагнёт при catch-up).
    */
  private def requireAccount(userId: UserId, store: EventStore): ZIO[Any, DomainError, Unit] =
    store.findUser(userId).flatMap { name =>
      if name.isDefined then ZIO.unit else ZIO.fail(AccountNotFound(userId.value.toString))
    }

  /** Безопасный парсинг userId: невалидное значение — `InvalidValue`. */
  private def parseUserId(value: String): ZIO[Any, DomainError, UserId] =
    ZIO.fromEither(UserId.from(value))

  /** Безопасный парсинг taskId: невалидное значение — `InvalidValue`. */
  private def parseTaskId(value: String): ZIO[Any, DomainError, TaskId] =
    ZIO.fromEither(TaskId.from(value))

  /** Безопасный парсинг UUID: невалидное значение — `InvalidValue`. */
  private def parseUuid(value: String): ZIO[Any, DomainError, UUID] =
    ZIO.fromEither(
      Try(UUID.fromString(value)).toEither.left.map(_ => InvalidValue("event", s"Invalid UUID: '$value'"))
    )
