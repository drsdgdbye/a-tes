package inc.uberpopug.notification

import java.time.Duration

import javax.sql.DataSource

import zio.{Clock, ZIO, ZIOAppDefault, ZLayer}
import zio.http.{handler, Method, Response, Routes, Server, Status}
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.prometheus
import zio.metrics.connectors.prometheus.PrometheusPublisher

import inc.uberpopug.notification.config.{AppConfig, KafkaConfig, TelegramChannelConfig}
import inc.uberpopug.notification.db.{DataSourceLayer, DbContext, Migrations}
import inc.uberpopug.notification.repository.NotificationStore
import inc.uberpopug.notification.service.{ChannelRegistry, NotificationConsumer, NotificationEventProcessor}

/** Точка входа aTES Notification Service: собирает ZLayer-граф, поднимает HTTP-сервер (health/ready/metrics) и consumer
  * событий.
  */
object Main extends ZIOAppDefault:
  /** Выделяет секцию `kafka` из AppConfig как отдельный слой для consumer-а. */
  private val kafkaConfig: ZLayer[AppConfig, Nothing, KafkaConfig] =
    ZLayer.fromFunction((cfg: AppConfig) => cfg.kafka)

  /** Выделяет секцию `channels.telegram` из AppConfig как отдельный слой для канала. */
  private val telegramConfig: ZLayer[AppConfig, Nothing, TelegramChannelConfig] =
    ZLayer.fromFunction((cfg: AppConfig) => cfg.channels.telegram)

  /** Единый Kafka-producer: публикация в DLQ. */
  private val producerLayer: ZLayer[AppConfig, Throwable, Producer] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[AppConfig]
        producer <- Producer.make(
          ProducerSettings(cfg.kafka.bootstrapServers).withClientId("ates-notification-producer")
        )
      yield producer
    }

  /** Период опроса metric registry для Prometheus. */
  private val metricsConfig: ZLayer[Any, Nothing, MetricsConfig] =
    ZLayer.succeed(MetricsConfig(Duration.ofSeconds(10)))

  /** Единый граф зависимостей приложения: конфиг, пул соединений, Quill-контекст, репозиторий, каналы, обработчик,
    * consumer и Prometheus. Собирается только здесь (SSOT композиции).
    */
  private val appLayer: ZLayer[
    Any,
    Throwable,
    AppConfig & DataSource & NotificationConsumer & PrometheusPublisher
  ] =
    ZLayer.make[
      AppConfig & DataSource & NotificationConsumer & PrometheusPublisher
    ](
      AppConfig.layer,
      DataSourceLayer.live,
      DbContext.live,
      NotificationStore.layer,
      ChannelRegistry.layer,
      NotificationEventProcessor.layer,
      NotificationConsumer.layer,
      NotificationConsumer.consumerLayer,
      producerLayer,
      kafkaConfig,
      telegramConfig,
      prometheus.publisherLayer,
      prometheus.prometheusLayer,
      metricsConfig,
      ZLayer.succeed(Clock.ClockLive)
    )

  /** Стартовая последовательность: миграции БД, запуск consumer-а событий в отдельном волокне и HTTP-сервер на порту из
    * конфига.
    */
  override def run: ZIO[Any, Throwable, Nothing] =
    (for
      cfg <- ZIO.service[AppConfig]
      ds <- ZIO.service[DataSource]
      _ <- Migrations.migrate(ds)
      consumer <- ZIO.service[NotificationConsumer]
      prometheus <- ZIO.service[PrometheusPublisher]
      _ <- ZIO.logInfo(s"Starting aTES Notification Service on port ${cfg.server.port}")
      routes = Routes(
        Method.GET / "health" -> handler(Response.json("""{"status":"ok"}""")),
        Method.GET / "ready" -> handler(readyHandler(ds)),
        Method.GET / "metrics" -> handler(prometheus.get.map(text => Response.text(text)))
      )
      _ <- consumer.run.fork
    yield Server
      .serve(routes)
      .provide(Server.defaultWithPort(cfg.server.port))).flatten.provideLayer(appLayer)

  /** `GET /ready` — проверка доступности БД. */
  private def readyHandler(ds: DataSource): ZIO[Any, Nothing, Response] =
    ZIO
      .attemptBlocking {
        val connection = ds.getConnection
        try connection.isValid(2)
        finally connection.close()
      }
      .map(valid =>
        if valid then Response.json("""{"status":"ok"}""")
        else Response.status(Status.ServiceUnavailable)
      )
      .orElse(ZIO.succeed(Response.status(Status.ServiceUnavailable)))
