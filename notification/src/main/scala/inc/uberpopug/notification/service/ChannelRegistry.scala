package inc.uberpopug.notification.service

import zio.{ZIO, ZLayer}

import inc.uberpopug.notification.config.TelegramChannelConfig
import inc.uberpopug.notification.domain.ChannelType

/** Канал доставки и его админские адреса (для уведомлений о невозможности доставки). */
final case class ChannelEntry(channel: NotificationChannel, adminAddresses: List[String])

/** Реестр каналов доставки: единственное место, где типы каналов связываются с реализациями (SSOT). Новый канал =
  * регистрация здесь.
  */
final case class ChannelRegistry(entries: Map[ChannelType, ChannelEntry]):
  /** Все зарегистрированные каналы. */
  def all: List[ChannelEntry] = entries.values.toList

object ChannelRegistry:
  /** Слой реестра: scoped-политики каналов живут столько же, сколько приложение. */
  val layer: ZLayer[TelegramChannelConfig, Nothing, ChannelRegistry] =
    ZLayer.scoped {
      for
        config <- ZIO.service[TelegramChannelConfig]
        telegram <- TelegramChannel.make(config)
      yield ChannelRegistry(
        Map(ChannelType.Telegram -> ChannelEntry(telegram, config.adminAddresses))
      )
    }
