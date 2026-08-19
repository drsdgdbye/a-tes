package inc.uberpopug.notification.repository

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import io.getquill.*
import zio.{ZIO, ZLayer}

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.PersistenceError
import inc.uberpopug.common.domain.UserId
import inc.uberpopug.notification.db.DbContext.Postgres
import inc.uberpopug.notification.domain.ChannelType

/** Строка таблицы `popug_contacts` в формате БД: адрес доставки попуга по каналу. */
final case class PopugContactRow(popugId: UUID, channel: String, address: String, createdAt: Instant)

/** Строка таблицы `sent_notifications` в формате БД: запись отправки (вставляется до отправки). Адрес — часть
  * дедупликации: одна доставка на (событие, канал, адресата), но каждый адресат канала получает своё сообщение.
  */
final case class SentNotificationRow(
    eventId: UUID,
    popugId: UUID,
    eventType: String,
    channel: String,
    address: String,
    sentAt: Instant
)

/** Строка таблицы `processed_events` в формате БД: идемпотентность обработки событий. */
final case class ProcessedEventRow(eventId: UUID, eventType: String, processedAt: Instant)

/** Хранилище Notification Service: контакты попугов, журнал отправок и идемпотентность. */
trait NotificationStore:
  /** Признак, что событие уже обработано до конца (прочитано из `processed_events`). */
  def isProcessed(eventId: UUID): ZIO[Any, DomainError, Boolean]

  /** Вставляет id события в `processed_events`; дубликат (SQLState 23505) — `false`. */
  def insertProcessedIfAbsent(eventId: UUID, eventType: String, at: Instant): ZIO[Any, DomainError, Boolean]

  /** Адрес попуга по каналу доставки; отсутствие маппинга — `None`. */
  def findAddress(popugId: UserId, channel: ChannelType): ZIO[Any, DomainError, Option[String]]

  /** Вставляет запись отправки в `sent_notifications` ДО фактической отправки (спека §7.4); дубликат — `false`.
    * Дедупликация по (event_id, channel, address): каждая доставка одному адресату записывается один раз.
    */
  def insertSentBeforeSend(
      eventId: UUID,
      popugId: UserId,
      eventType: String,
      channel: ChannelType,
      address: String,
      at: Instant
  ): ZIO[Any, DomainError, Boolean]

  /** Удаляет маркер `sent_notifications` после неудачной отправки, чтобы retry/DLQ-replay мог повторить доставку. */
  def deleteSentBeforeRetry(
      eventId: UUID,
      channel: ChannelType,
      address: String
  ): ZIO[Any, DomainError, Unit]

object NotificationStore:
  /** Слой репозитория поверх Quill-контекста Postgres. */
  val layer: ZLayer[Postgres, Nothing, NotificationStore] =
    ZLayer.fromFunction(NotificationStoreLive(_))

/** Явный маппинг row-классов на таблицы: имена таблиц из спеки (popug_contacts, sent_notifications, processed_events),
  * а не дефолтные `*_row` от Quill.
  */
object Tables:
  import io.getquill.{SchemaMeta, schemaMeta}

  inline implicit def popugContactsSchema: SchemaMeta[PopugContactRow] = schemaMeta("popug_contacts")
  inline implicit def sentNotificationsSchema: SchemaMeta[SentNotificationRow] = schemaMeta("sent_notifications")
  inline implicit def processedEventsSchema: SchemaMeta[ProcessedEventRow] = schemaMeta("processed_events")

/** Quill-реализация хранилища поверх Postgres. */
final case class NotificationStoreLive(ctx: Postgres) extends NotificationStore:
  import ctx.*
  import Tables.given

  /** Оборачивает SQL-ошибку в `PersistenceError`. */
  private def toPersistenceError(ex: Throwable): DomainError =
    ex match
      case e: SQLException => PersistenceError(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
      case other           => PersistenceError(Option(other.getMessage).getOrElse(other.getClass.getSimpleName))

  def isProcessed(eventId: UUID): ZIO[Any, DomainError, Boolean] =
    run(query[ProcessedEventRow].filter(_.eventId == lift(eventId)).nonEmpty)
      .mapError(toPersistenceError)

  def insertProcessedIfAbsent(eventId: UUID, eventType: String, at: Instant): ZIO[Any, DomainError, Boolean] =
    run(query[ProcessedEventRow].insertValue(lift(ProcessedEventRow(eventId, eventType, at))))
      .as(true)
      .catchSome { case e: SQLException if e.getSQLState == "23505" => ZIO.succeed(false) }
      .mapError(toPersistenceError)

  def findAddress(popugId: UserId, channel: ChannelType): ZIO[Any, DomainError, Option[String]] =
    run(
      query[PopugContactRow]
        .filter(r => r.popugId == lift(popugId.value) && r.channel == lift(channel.wire))
    )
      .map(_.headOption.map(_.address))
      .mapError(toPersistenceError)

  def insertSentBeforeSend(
      eventId: UUID,
      popugId: UserId,
      eventType: String,
      channel: ChannelType,
      address: String,
      at: Instant
  ): ZIO[Any, DomainError, Boolean] =
    run(
      query[SentNotificationRow].insertValue(
        lift(SentNotificationRow(eventId, popugId.value, eventType, channel.wire, address, at))
      )
    )
      .as(true)
      .catchSome { case e: SQLException if e.getSQLState == "23505" => ZIO.succeed(false) }
      .mapError(toPersistenceError)

  def deleteSentBeforeRetry(
      eventId: UUID,
      channel: ChannelType,
      address: String
  ): ZIO[Any, DomainError, Unit] =
    run(
      query[SentNotificationRow]
        .filter(r => r.eventId == lift(eventId) && r.channel == lift(channel.wire) && r.address == lift(address))
        .delete
    ).unit
      .mapError(toPersistenceError)
