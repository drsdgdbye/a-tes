package inc.uberpopug.accounting.config

import zio.{Config, ZLayer}
import zio.config.magnolia.deriveConfig
import zio.config.typesafe.TypesafeConfigProvider

/** Полный конфиг сервиса, читается из секции `ates` в `application.conf`. */
final case class AppConfig(
    server: ServerConfig,
    database: DatabaseConfig,
    kafka: KafkaConfig,
    outbox: OutboxConfig,
    payout: PayoutConfig
)

/** HTTP-настройки сервера. */
final case class ServerConfig(port: Int)

/** Настройки подключения к Postgres и пула HikariCP. */
final case class DatabaseConfig(url: String, user: String, password: String, maxPoolSize: Int)

/** Настройки Kafka: брокеры, группа consumer-а, топики событий и DLQ. */
final case class KafkaConfig(
    bootstrapServers: List[String],
    consumerGroupId: String,
    topicUserCreated: String,
    topicTaskCreated: String,
    topicTaskAssigned: String,
    topicTaskCompleted: String,
    topicPaymentProcessed: String,
    topicDlq: String
)

/** Настройки polling-цикла transactional outbox. */
final case class OutboxConfig(batchSize: Int, pollIntervalSeconds: Long)

/** Настройки ежедневных выплат: `useUtc = true` — QUARTZ-cron в UTC, иначе фиксированный интервал. */
final case class PayoutConfig(useUtc: Boolean, cronExpression: String, intervalSeconds: Long)

object AppConfig:
  /** Дескриптор конфига, выводимый из case classes с корнем `ates`. */
  val config: Config[AppConfig] = deriveConfig[AppConfig].nested("ates")

  /** Слой загрузки конфига из classpath-ресурса с env-оверрайдами `${?ATES_*}`. */
  val layer: ZLayer[Any, Throwable, AppConfig] =
    ZLayer.fromZIO(TypesafeConfigProvider.fromResourcePath().load(config))
