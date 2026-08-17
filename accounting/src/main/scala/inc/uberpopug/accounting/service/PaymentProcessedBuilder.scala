package inc.uberpopug.accounting.service

import java.time.{Instant, LocalDate}
import java.util.UUID

import inc.uberpopug.accounting.repository.OutboxRecord
import inc.uberpopug.common.domain.{Money, UserId}

import accounting.payment_processed.PaymentProcessed

/** Построение события `PaymentProcessed` (protobuf) для публикации в Kafka. */
object PaymentProcessedBuilder:
  /** Тип события для outbox и маппинга на топик. */
  val EventType = "PaymentProcessed"

  /** `PaymentProcessed`: выплата попугу в конце дня. `eventId` — детерминированный id выплаты. */
  def paymentProcessed(
      userId: UserId,
      popugName: String,
      amount: Money,
      date: LocalDate,
      eventId: UUID,
      now: Instant
  ): OutboxRecord =
    val event = PaymentProcessed(
      eventId = eventId.toString,
      timestamp = now.toEpochMilli,
      version = 1,
      popugId = userId.value.toString,
      popugName = popugName,
      amountCents = amount.toCents,
      date = date.toString
    )
    OutboxRecord(userId.value, EventType, event.toByteArray, now)
