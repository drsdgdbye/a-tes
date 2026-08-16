package inc.uberpopug.taskservice.service

import java.time.Instant
import java.util.UUID

import inc.uberpopug.common.domain.UserId
import inc.uberpopug.taskservice.domain.Task
import inc.uberpopug.taskservice.repository.OutboxRecord

import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted
import task.task_created.TaskCreated

/** Типы доменных событий Task Service: используются как маркеры в outbox и для маппинга на топики Kafka. */
object TaskEventTypes:
  /** Задача создана (публикуется вместе с TaskAssigned). */
  val TaskCreated = "TaskCreated"

  /** Первичный ассайн или реассайн при перетасовке. */
  val TaskAssigned = "TaskAssigned"

  /** Исполнитель отметил задачу выполненной. */
  val TaskCompleted = "TaskCompleted"

/** Построение доменных событий (protobuf) и записей outbox. */
object TaskEventBuilders:
  /** `TaskCreated`: создание задачи с уже назначенным исполнителем. */
  def taskCreated(task: Task, now: Instant): OutboxRecord =
    val event = TaskCreated(
      eventId = UUID.randomUUID().toString,
      timestamp = now.toEpochMilli,
      version = task.version.toInt,
      taskId = task.id.value.toString,
      title = task.title.value,
      description = task.description.map(_.value).getOrElse(""),
      assigneeId = task.assigneeId.value.toString,
      assignFeeCents = task.assignFee.toCents,
      completeRewardCents = task.completeReward.toCents
    )
    OutboxRecord(task.id.value, TaskEventTypes.TaskCreated, event.toByteArray, now)

  /** `TaskAssigned`: первичный ассайн (`oldAssigneeId = None`) или реассайн при перетасовке. */
  def taskAssigned(task: Task, oldAssigneeId: Option[UserId], now: Instant): OutboxRecord =
    val event = TaskAssigned(
      eventId = UUID.randomUUID().toString,
      timestamp = now.toEpochMilli,
      version = task.version.toInt,
      taskId = task.id.value.toString,
      taskTitle = task.title.value,
      newAssigneeId = task.assigneeId.value.toString,
      oldAssigneeId = oldAssigneeId.map(_.value.toString).getOrElse(""),
      assignFeeCents = task.assignFee.toCents
    )
    OutboxRecord(task.id.value, TaskEventTypes.TaskAssigned, event.toByteArray, now)

  /** `TaskCompleted`: исполнитель отметил задачу выполненной. */
  def taskCompleted(task: Task, now: Instant): OutboxRecord =
    val event = TaskCompleted(
      eventId = UUID.randomUUID().toString,
      timestamp = now.toEpochMilli,
      version = task.version.toInt,
      taskId = task.id.value.toString,
      taskTitle = task.title.value,
      assigneeId = task.assigneeId.value.toString,
      completeRewardCents = task.completeReward.toCents
    )
    OutboxRecord(task.id.value, TaskEventTypes.TaskCompleted, event.toByteArray, now)
