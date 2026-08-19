package inc.uberpopug.notification

import java.time.Instant
import java.util.UUID

import zio.{Ref, ZIO}

import inc.uberpopug.common.domain.{DomainError, UserId}
import inc.uberpopug.notification.domain.ChannelType
import inc.uberpopug.notification.repository.NotificationStore

/** Упрощает `.orDie` для типизированных ошибок `DomainError` (не `Throwable`): превращает ошибку в исключение и роняет
  * эффект — «этого не должно случиться в тесте».
  */
extension [R, A](io: ZIO[R, DomainError, A])
  def orDieE: ZIO[R, Nothing, A] = io.mapError(error => new RuntimeException(error.toString)).orDie

/** In-memory состояние хранилища Notification Service: контакты попугов, журнал отправок и идемпотентность. */
final case class InMemoryNotificationState(
    contacts: Map[(UUID, String), String] = Map.empty,
    sent: Set[(UUID, String, String)] = Set.empty,
    processed: Set[UUID] = Set.empty
)

/** In-memory реализация `NotificationStore` на Ref с той же семантикой, что и Quill-реализация: dedup по `eventId` и по
  * `(eventId, channel, address)`, отсутствие маппинга — `None`.
  */
final case class InMemoryNotificationStore(state: Ref[InMemoryNotificationState]) extends NotificationStore:
  def isProcessed(eventId: UUID): ZIO[Any, DomainError, Boolean] =
    state.get.map(_.processed.contains(eventId))

  def insertProcessedIfAbsent(eventId: UUID, eventType: String, at: Instant): ZIO[Any, DomainError, Boolean] =
    state.modify { s =>
      if s.processed.contains(eventId) then (false, s)
      else (true, s.copy(processed = s.processed + eventId))
    }

  def findAddress(popugId: UserId, channel: ChannelType): ZIO[Any, DomainError, Option[String]] =
    state.get.map(_.contacts.get((popugId.value, channel.wire)))

  def insertSentBeforeSend(
      eventId: UUID,
      popugId: UserId,
      eventType: String,
      channel: ChannelType,
      address: String,
      at: Instant
  ): ZIO[Any, DomainError, Boolean] =
    state.modify { s =>
      val key = (eventId, channel.wire, address)
      if s.sent.contains(key) then (false, s)
      else (true, s.copy(sent = s.sent + key))
    }

  def deleteSentBeforeRetry(eventId: UUID, channel: ChannelType, address: String): ZIO[Any, DomainError, Unit] =
    state.update(s => s.copy(sent = s.sent - ((eventId, channel.wire, address))))
