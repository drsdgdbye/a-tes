package inc.uberpopug.notification.config

import zio.{Config, ZLayer}
import zio.config.magnolia.deriveConfig
import zio.config.typesafe.TypesafeConfigProvider

/** Полный конфиг сервиса, читается из секции `ates` в `application.conf`. */
final case class AppConfig(
    server: ServerConfig,
    database: DatabaseConfig,
    kafka: KafkaConfig,
    channels: ChannelsConfig
)

/** HTTP-настройки сервера. */
final case class ServerConfig(port: Int)

/** Настройки подключения к Postgres и пула HikariCP. */
final case class DatabaseConfig(url: String, user: String, password: String, maxPoolSize: Int)

/** Настройки Kafka: брокеры, группа consumer-а, топики событий и DLQ. */
final case class KafkaConfig(
    bootstrapServers: List[String],
    consumerGroupId: String,
    topicTaskAssigned: String,
    topicTaskCompleted: String,
    topicPaymentProcessed: String,
    topicDlq: String
)

/** Секции каналов доставки. Новый канал = новый case class + секция `ates.channels.*`. */
final case class ChannelsConfig(telegram: TelegramChannelConfig)

/** Настройки Telegram-канала: токен бота, админские адреса для уведомлений о невозможности доставки и
  * resilience-параметры (спека §7.2). Токен и админские адреса задаются только через env при запуске контейнера.
  */
final case class TelegramChannelConfig(
    botToken: String,
    adminAddresses: List[String],
    rateLimitPerSecond: Int,
    sendTimeoutSeconds: Int,
    retryAttempts: Int
)

object AppConfig:
  /** Дескриптор конфига с корнем `ates`. `adminAddresses` читается как CSV-строка (env-дружелюбно). */
  val config: Config[AppConfig] =
    (
      (
        deriveConfig[ServerConfig].nested("server") zip
          deriveConfig[DatabaseConfig].nested("database") zip
          deriveConfig[KafkaConfig].nested("kafka") zip
          telegramConfig.nested("channels", "telegram")
      ).map { case (server, database, kafka, telegram) =>
        AppConfig(server, database, kafka, ChannelsConfig(telegram))
      }
    ).nested("ates")

  /** Конфиг Telegram-канала; `adminAddresses` — список адресов через запятую, пустые элементы отбрасываются. */
  private val telegramConfig: Config[TelegramChannelConfig] =
    (
      Config.string("botToken") zip
        Config.string("adminAddresses").map(_.split(",").map(_.trim).filter(_.nonEmpty).toList) zip
        Config.int("rateLimitPerSecond") zip
        Config.int("sendTimeoutSeconds") zip
        Config.int("retryAttempts")
    ).map { case (botToken, adminAddresses, rateLimitPerSecond, sendTimeoutSeconds, retryAttempts) =>
      TelegramChannelConfig(botToken, adminAddresses, rateLimitPerSecond, sendTimeoutSeconds, retryAttempts)
    }

  /** Слой загрузки конфига из classpath-ресурса с env-оверрайдами `${?ATES_*}`. */
  val layer: ZLayer[Any, Throwable, AppConfig] =
    ZLayer.fromZIO(TypesafeConfigProvider.fromResourcePath().load(config))
