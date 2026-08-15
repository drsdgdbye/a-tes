package inc.uberpopug.auth.config

import zio.{Config, ZLayer}
import zio.config.magnolia.deriveConfig
import zio.config.typesafe.TypesafeConfigProvider

/** Полный конфиг сервиса, читается из секции `ates` в `application.conf`. */
final case class AppConfig(
    server: ServerConfig,
    database: DatabaseConfig,
    kafka: KafkaConfig,
    jwt: JwtConfig,
    outbox: OutboxConfig,
    auth: AuthConfig
)

/** HTTP-настройки сервера. */
final case class ServerConfig(port: Int)

/** Настройки подключения к Postgres и пула HikariCP. */
final case class DatabaseConfig(url: String, user: String, password: String, maxPoolSize: Int)

/** Настройки Kafka: брокеры и топик публикации `UserCreated`. */
final case class KafkaConfig(bootstrapServers: List[String], topicUserCreated: String)

/** Настройки JWT: issuer и сроки жизни access/refresh токенов. */
final case class JwtConfig(issuer: String, accessTtlSeconds: Long, refreshTtlSeconds: Long)

/** Настройки polling-цикла transactional outbox. */
final case class OutboxConfig(batchSize: Int, pollIntervalSeconds: Long)

/** Настройки аутентификации: открытая саморегистрация пользователей. */
final case class AuthConfig(registrationEnabled: Boolean)

object AppConfig:
  /** Дескриптор конфига, выводимый из case classes с корнем `ates`. */
  val config: Config[AppConfig] = deriveConfig[AppConfig].nested("ates")

  /** Слой загрузки конфига из classpath-ресурса с env-оверрайдами `${?ATES_*}`. */
  val layer: ZLayer[Any, Throwable, AppConfig] =
    ZLayer.fromZIO(TypesafeConfigProvider.fromResourcePath().load(config))
