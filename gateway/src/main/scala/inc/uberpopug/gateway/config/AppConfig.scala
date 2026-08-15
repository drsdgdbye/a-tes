package inc.uberpopug.gateway.config

import zio.{Config, ZLayer}
import zio.config.magnolia.deriveConfig
import zio.config.typesafe.TypesafeConfigProvider

/** Полный конфиг Gateway, читается из секции `ates` в `application.conf`. */
final case class AppConfig(
    server: ServerConfig,
    jwt: GatewayJwtConfig,
    services: ServicesConfig,
    resilience: ResilienceConfig
)

/** HTTP-настройки сервера. */
final case class ServerConfig(port: Int)

/** Настройки локальной верификации JWT: источник ключей Auth и интервалы обновления. */
final case class GatewayJwtConfig(
    issuer: String,
    keysUrl: String,
    refreshIntervalSeconds: Long,
    startupRetryIntervalSeconds: Long
)

/** Base URL'ы downstream-сервисов, в которые Gateway проксирует запросы. */
final case class ServicesConfig(
    auth: ServiceConfig,
    taskService: ServiceConfig,
    accounting: ServiceConfig,
    analytics: ServiceConfig
)

/** Base URL отдельного downstream-сервиса. */
final case class ServiceConfig(baseUrl: String)

/** Параметры resilience-политик (rezilience) для каждого downstream-сервиса. */
final case class ResilienceConfig(
    circuitBreaker: CircuitBreakerConfig,
    retry: RetryConfig,
    timeLimiter: TimeLimiterConfig,
    bulkhead: BulkheadConfig,
    rateLimiter: RateLimiterConfig
)

/** CircuitBreaker: N последовательных ошибок → open, сброс в half-open через фиксированный интервал. */
final case class CircuitBreakerConfig(maxFailures: Int, resetIntervalSeconds: Long)

/** Retry только для транзиентных ошибок: exponential backoff с ограничением попыток. */
final case class RetryConfig(maxRetries: Int, initialDelayMillis: Long, backoffFactor: Double)

/** TimeLimiter: чтение и запись — разные таймауты. */
final case class TimeLimiterConfig(readTimeoutSeconds: Long, writeTimeoutSeconds: Long)

/** Bulkhead: максимум одновременных запросов к сервису. */
final case class BulkheadConfig(maxConcurrent: Int)

/** RateLimiter: лимит запросов в секунду к сервису. */
final case class RateLimiterConfig(rps: Int)

object AppConfig:
  /** Дескриптор конфига, выводимый из case classes с корнем `ates`. */
  val config: Config[AppConfig] = deriveConfig[AppConfig].nested("ates")

  /** Слой загрузки конфига из classpath-ресурса с env-оверрайдами `${?ATES_*}`. */
  val layer: ZLayer[Any, Throwable, AppConfig] =
    ZLayer.fromZIO(TypesafeConfigProvider.fromResourcePath().load(config))
