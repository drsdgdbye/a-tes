package inc.uberpopug.auth.service

import java.time.Instant
import java.util.UUID

import auth.user_created.UserCreated

import inc.uberpopug.auth.domain.User
import inc.uberpopug.auth.repository.OutboxRecord

/** Построение события `UserCreated` (protobuf) и его записи в outbox. */
object UserCreatedEvent:
  /** Сериализует создание пользователя в protobuf-событие `UserCreated` и возвращает запись outbox с ключом aggregateId =
    * id пользователя.
    */
  def of(user: User, now: Instant): OutboxRecord =
    val event = UserCreated(
      eventId = UUID.randomUUID().toString,
      timestamp = now.toEpochMilli,
      version = 1,
      userId = user.id.value.toString,
      name = user.name,
      email = user.email.value,
      role = user.role.wire
    )
    OutboxRecord(
      aggregateId = user.id.value,
      eventType = "UserCreated",
      payload = event.toByteArray,
      createdAt = now
    )
