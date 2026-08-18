package inc.uberpopug.notification.domain

/** Готовое к отправке сообщение: канал, адрес получателя и текст. */
final case class RenderedMessage(channel: ChannelType, address: String, text: String)
