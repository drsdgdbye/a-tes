package inc.uberpopug.notification.domain

import java.time.{Instant, LocalDate}

import inc.uberpopug.common.domain.UserId

/** Событие, порождающее уведомление, в доменных терминах. Маппится из protobuf-событий в `NotificationEventProcessor`;
  * содержит только поля, нужные для текста сообщения и адресата.
  */
enum NotificationEvent(val popugId: UserId, val timestamp: Instant):
  /** Задача назначена попугу (`TaskAssigned.new_assignee_id`). */
  case TaskAssigned(
      override val popugId: UserId,
      taskTitle: String,
      override val timestamp: Instant
  ) extends NotificationEvent(popugId, timestamp)

  /** Задача выполнена исполнителем (`TaskCompleted.assignee_id`). */
  case TaskCompleted(
      override val popugId: UserId,
      taskTitle: String,
      rewardCents: Long,
      override val timestamp: Instant
  ) extends NotificationEvent(popugId, timestamp)

  /** Произведена ежедневная выплата (`PaymentProcessed.popug_id`). */
  case PaymentProcessed(
      override val popugId: UserId,
      amountCents: Long,
      date: LocalDate,
      override val timestamp: Instant
  ) extends NotificationEvent(popugId, timestamp)
