package inc.uberpopug.notification.service

import zio.ZIO

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.notification.domain.{ChannelType, RenderedMessage}

/** Канал доставки уведомлений. Новый канал (email/sms) = реализация трейта + регистрация в `ChannelRegistry` без
  * изменения пайплайна.
  */
trait NotificationChannel:
  /** Тип канала (используется в БД и конфиге). */
  def channelType: ChannelType

  /** Отправляет готовое сообщение; ошибки доставки — `TelegramSendFailed` (после внутренних retry). */
  def send(message: RenderedMessage): ZIO[Any, DomainError, Unit]
