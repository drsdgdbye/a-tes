package inc.uberpopug.accounting.service

import java.time.Instant
import java.util.UUID

import inc.uberpopug.common.domain.{Money, TaskId, UserId}
import inc.uberpopug.accounting.domain.AccountEvent

import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted
import task.task_created.TaskCreated

/** Чистый маппинг Kafka-событий TaskService/Auth в доменные события event store. `timestamp` берётся из самого события
  * (детерминированность срезов по дате), `eventId` — новый UUID события event store.
  */
object AccountingEventMapper:
  /** `TaskCreated` → `TaskPriceRecorded`: цены зафиксированы при создании задачи. */
  def taskPriceRecorded(event: TaskCreated): AccountEvent =
    AccountEvent.TaskPriceRecorded(
      eventId = UUID.randomUUID(),
      timestamp = Instant.ofEpochMilli(event.timestamp),
      taskId = TaskId(UUID.fromString(event.taskId)),
      userId = UserId(UUID.fromString(event.assigneeId)),
      assignFee = Money.fromCents(event.assignFeeCents),
      completeReward = Money.fromCents(event.completeRewardCents)
    )

  /** `TaskAssigned` → события списания (и возврата, если был старый исполнитель). Порядок: сначала возврат старому
    * исполнителю, затем списание с нового.
    */
  def assignedEvents(event: TaskAssigned): List[AccountEvent] =
    val timestamp = Instant.ofEpochMilli(event.timestamp)
    val taskId = TaskId(UUID.fromString(event.taskId))
    val refund =
      if event.oldAssigneeId.nonEmpty then
        List(
          AccountEvent.AccountCredited(
            eventId = UUID.randomUUID(),
            timestamp = timestamp,
            userId = UserId(UUID.fromString(event.oldAssigneeId)),
            amount = Money.fromCents(event.assignFeeCents),
            taskId = taskId,
            reason = AccountEvent.CreditReason.AssignmentRefund
          )
        )
      else Nil
    refund :+ AccountEvent.AccountDebited(
      eventId = UUID.randomUUID(),
      timestamp = timestamp,
      userId = UserId(UUID.fromString(event.newAssigneeId)),
      amount = Money.fromCents(event.assignFeeCents),
      taskId = taskId,
      reason = AccountEvent.DebitReason.TaskAssigned
    )

  /** `TaskCompleted` → `AccountCredited` с CompleteReward исполнителю. */
  def completedReward(event: TaskCompleted): AccountEvent =
    AccountEvent.AccountCredited(
      eventId = UUID.randomUUID(),
      timestamp = Instant.ofEpochMilli(event.timestamp),
      userId = UserId(UUID.fromString(event.assigneeId)),
      amount = Money.fromCents(event.completeRewardCents),
      taskId = TaskId(UUID.fromString(event.taskId)),
      reason = AccountEvent.CreditReason.TaskCompleted
    )
