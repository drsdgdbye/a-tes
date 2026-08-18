package inc.uberpopug.notification.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Канал доставки уведомлений. Wire-значение используется в БД (`popug_contacts.channel`, `sent_notifications.channel`)
  * и в конфиге. Новый канал (email/sms) = новый case + реализация `NotificationChannel` + секция `ates.channels.*`.
  */
enum ChannelType(val wire: String):
  case Telegram extends ChannelType("telegram")

object ChannelType:
  /** Парсит канал из строки; неизвестное значение — ошибка. */
  def from(value: String): Either[DomainError, ChannelType] =
    ChannelType.values
      .find(_.wire == value.trim.toLowerCase)
      .toRight(InvalidValue("channel", s"Invalid channel: '$value'"))
