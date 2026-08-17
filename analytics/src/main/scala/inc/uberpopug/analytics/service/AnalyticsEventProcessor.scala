package inc.uberpopug.analytics.service

import java.time.{Instant, LocalDate}
import java.util.UUID

import scala.util.Try

import zio.ZIO

import inc.uberpopug.analytics.repository.AnalyticsStore
import inc.uberpopug.common.domain.{DomainError, TaskId, UserId}
import inc.uberpopug.common.domain.DomainError.{AccountNotFound, InvalidValue}

import accounting.payment_processed.PaymentProcessed
import auth.user_created.UserCreated
import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted
import task.task_created.TaskCreated

/** Чистая обработка одного Kafka-события: безопасный парсинг примитивов (невалидные значения — `InvalidValue` через
  * typed error channel, без исключений) и применение к read-side проекциям. Вынесена из consumer-а для тестируемости
  * без Kafka.
  */
object AnalyticsEventProcessor:
  /** `UserCreated` → проекция попуга с нулевым балансом + `daily_stats.popugs_total`. */
  def processUserCreated(event: UserCreated, store: AnalyticsStore): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      userId <- parseUserId(event.userId)
      _ <- store.upsertUser(eventId, userId, event.name, Instant.ofEpochMilli(event.timestamp))
    yield ()

  /** `TaskCreated` → задача в статусе `open` + `daily_stats.earnings` за дату создания. */
  def processTaskCreated(event: TaskCreated, store: AnalyticsStore): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      taskId <- parseTaskId(event.taskId)
      _ <- store.insertTask(
        eventId,
        taskId,
        event.title,
        event.assignFeeCents,
        event.completeRewardCents,
        Instant.ofEpochMilli(event.timestamp)
      )
    yield ()

  /** `TaskAssigned` → списание с нового исполнителя и возврат старому (если был). Строки попугов должны существовать.
    */
  def processTaskAssigned(event: TaskAssigned, store: AnalyticsStore): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      newAssignee <- parseUserId(event.newAssigneeId)
      oldAssignee <- if event.oldAssigneeId.nonEmpty then parseUserId(event.oldAssigneeId).map(Some(_)) else ZIO.none
      _ <- requirePopug(newAssignee, store)
      _ <- ZIO.foreachDiscard(oldAssignee)(assignee => requirePopug(assignee, store))
      _ <- store.applyAssignment(
        eventId,
        newAssignee,
        oldAssignee,
        event.assignFeeCents,
        Instant.ofEpochMilli(event.timestamp)
      )
    yield ()

  /** `TaskCompleted` → задача закрыта + начисление `completeReward` исполнителю. Строка попуга должна существовать. */
  def processTaskCompleted(event: TaskCompleted, store: AnalyticsStore): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      taskId <- parseTaskId(event.taskId)
      assigneeId <- parseUserId(event.assigneeId)
      _ <- requirePopug(assigneeId, store)
      _ <- store.completeTask(
        eventId,
        taskId,
        assigneeId,
        event.completeRewardCents,
        Instant.ofEpochMilli(event.timestamp)
      )
    yield ()

  /** `PaymentProcessed` → баланс `-= amount`, имя из события, `daily_stats.popugs_negative` за дату выплаты. */
  def processPaymentProcessed(event: PaymentProcessed, store: AnalyticsStore): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      popugId <- parseUserId(event.popugId)
      date <- parseDate(event.date)
      _ <- requirePopug(popugId, store)
      _ <- store.applyPayout(
        eventId,
        popugId,
        event.popugName,
        event.amountCents,
        date,
        Instant.ofEpochMilli(event.timestamp)
      )
    yield ()

  /** Событие для ещё не существующего попуга — транзиентная ошибка: при восстановлении `auth.user.created` приходит
    * позже (catch-up), поток прерывается и событие переобрабатывается после переподписки.
    */
  private def requirePopug(userId: UserId, store: AnalyticsStore): ZIO[Any, DomainError, Unit] =
    store.findPopug(userId).flatMap { name =>
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

  /** Безопасный парсинг ISO-даты: невалидное значение — `InvalidValue`. */
  private def parseDate(value: String): ZIO[Any, DomainError, LocalDate] =
    ZIO
      .attempt(LocalDate.parse(value))
      .mapError(_ => InvalidValue("date", s"Invalid ISO-8601 date: '$value'"))
