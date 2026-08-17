package inc.uberpopug.accounting.service

import java.time.Instant
import java.util.UUID

import inc.uberpopug.common.domain.{Money, TaskId, UserId}
import inc.uberpopug.accounting.domain.AccountEvent

/** Чистый маппинг типизированных данных Kafka-событий TaskService/Auth в доменные события event store. `timestamp`
  * берётся из самого события (детерминированность срезов по дате), `eventId` — новый UUID события event store. Парсинг
  * примитивов из protobuf выполняется в `AccountingEventProcessor` (безопасно, через typed error channel); сюда
  * приходят уже валидные значения.
  */
object AccountingEventMapper:
  /** Цены задачи → `TaskPriceRecorded`. */
  def taskPriceRecorded(
      timestamp: Instant,
      taskId: TaskId,
      userId: UserId,
      assignFee: Money,
      completeReward: Money
  ): AccountEvent =
    AccountEvent.TaskPriceRecorded(
      eventId = UUID.randomUUID(),
      timestamp = timestamp,
      taskId = taskId,
      userId = userId,
      assignFee = assignFee,
      completeReward = completeReward
    )

  /** События списания и возврата (если был старый исполнитель). Порядок: сначала возврат старому, затем списание с
    * нового.
    */
  def assignedEvents(
      timestamp: Instant,
      taskId: TaskId,
      newAssignee: UserId,
      oldAssignee: Option[UserId],
      assignFee: Money
  ): List[AccountEvent] =
    val refund =
      oldAssignee match
        case Some(assignee) =>
          List(
            AccountEvent.AccountCredited(
              eventId = UUID.randomUUID(),
              timestamp = timestamp,
              userId = assignee,
              amount = assignFee,
              taskId = taskId,
              reason = AccountEvent.CreditReason.AssignmentRefund
            )
          )
        case None => Nil
    refund :+ AccountEvent.AccountDebited(
      eventId = UUID.randomUUID(),
      timestamp = timestamp,
      userId = newAssignee,
      amount = assignFee,
      taskId = taskId,
      reason = AccountEvent.DebitReason.TaskAssigned
    )

  /** CompleteReward → `AccountCredited` исполнителю. */
  def completedReward(
      timestamp: Instant,
      taskId: TaskId,
      userId: UserId,
      completeReward: Money
  ): AccountEvent =
    AccountEvent.AccountCredited(
      eventId = UUID.randomUUID(),
      timestamp = timestamp,
      userId = userId,
      amount = completeReward,
      taskId = taskId,
      reason = AccountEvent.CreditReason.TaskCompleted
    )
