package inc.uberpopug.notification.service

import java.util.UUID

import zio.{Clock, ZIO, ZLayer}

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.notification.domain.{
  ChannelType,
  NotificationEvent,
  NotificationTextBuilder,
  RenderedMessage,
  StalenessPolicy
}
import inc.uberpopug.notification.repository.NotificationStore

/** Обработка одного Kafka-события в доменных терминах: идемпотентность, защита от лавины и доставка по всем каналам.
  * Чистый относительно Kafka/HTTP — вынесен для тестируемости без внешних систем.
  */
trait NotificationEventProcessor:
  def process(eventId: UUID, eventType: String, event: NotificationEvent): ZIO[Any, DomainError, Unit]

object NotificationEventProcessor:
  /** Слой обработчика: store, реестр каналов и Clock (время — через `Clock` для TestClock). */
  val layer: ZLayer[NotificationStore & ChannelRegistry & Clock, Nothing, NotificationEventProcessor] =
    ZLayer.fromFunction(NotificationEventProcessorLive(_, _, _))

/** Реализация пайплайна: ① dedup через `processed_events` → ② staleness (старше 5 минут — не слать никому) → ③ по
  * каждому каналу: адрес попуга есть → отправка; нет → админские адреса канала; админских нет → лог.
  */
final case class NotificationEventProcessorLive(
    store: NotificationStore,
    registry: ChannelRegistry,
    clock: Clock
) extends NotificationEventProcessor:

  def process(eventId: UUID, eventType: String, event: NotificationEvent): ZIO[Any, DomainError, Unit] =
    for
      now <- clock.instant
      fresh <- store.insertProcessedIfAbsent(eventId, eventType, now)
      _ <-
        if !fresh then ZIO.logInfo(s"Skip duplicate event $eventId ($eventType)")
        else if StalenessPolicy.isStale(event.timestamp, now) then
          ZIO.logInfo(
            s"Skip stale event $eventId ($eventType): event is older than ${StalenessPolicy.maxEventAge}"
          )
        else notifyChannels(eventId, eventType, event, now)
    yield ()

  /** Доставка по каждому каналу реестра: попугу или админским адресам при отсутствии маппинга. */
  private def notifyChannels(
      eventId: UUID,
      eventType: String,
      event: NotificationEvent,
      now: java.time.Instant
  ): ZIO[Any, DomainError, Unit] =
    ZIO.foreachDiscard(registry.all) { entry =>
      for
        address <- store.findAddress(event.popugId, entry.channel.channelType)
        _ <- address match
          case Some(addr) =>
            deliver(eventId, eventType, event, entry.channel, addr, NotificationTextBuilder.render(event), now)
          case None =>
            if entry.adminAddresses.isEmpty then
              ZIO.logWarning(
                s"No ${entry.channel.channelType.wire} address for popug ${event.popugId.value} " +
                  s"and no admin addresses configured, event $eventId ($eventType)"
              )
            else
              val adminText = adminMessage(eventType, event, entry.channel.channelType)
              ZIO.foreachDiscard(entry.adminAddresses) { adminAddr =>
                deliver(eventId, eventType, event, entry.channel, adminAddr, adminText, now)
              }
      yield ()
    }

  /** Записывает намерение отправить (INSERT до отправки, спека §7.4) и отправляет; дубликат (23505) → пропуск. */
  private def deliver(
      eventId: UUID,
      eventType: String,
      event: NotificationEvent,
      channel: NotificationChannel,
      address: String,
      text: String,
      now: java.time.Instant
  ): ZIO[Any, DomainError, Unit] =
    for
      recorded <- store.insertSentBeforeSend(eventId, event.popugId, eventType, channel.channelType, address, now)
      _ <-
        if !recorded then ZIO.logInfo(s"Notification for $eventId already sent via ${channel.channelType.wire}")
        else channel.send(RenderedMessage(channel.channelType, address, text))
    yield ()

  /** Сообщение админам о невозможности доставки попугу. */
  private def adminMessage(eventType: String, event: NotificationEvent, channel: ChannelType): String =
    s"Не удалось доставить уведомление ($eventType) попугу ${event.popugId.value} по каналу ${channel.wire}: " +
      "не указан адрес получателя"
