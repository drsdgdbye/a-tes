package inc.uberpopug.analytics

import java.time.Duration

import javax.sql.DataSource

import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.{Clock, ZIO, ZIOAppDefault, ZLayer}
import zio.http.{handler, Method, Response, Routes, Server}
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.prometheus
import zio.metrics.connectors.prometheus.PrometheusPublisher

import inc.uberpopug.analytics.api.AnalyticsServerLogic
import inc.uberpopug.analytics.config.{AppConfig, KafkaConfig}
import inc.uberpopug.analytics.db.{DataSourceLayer, DbContext, Migrations}
import inc.uberpopug.analytics.repository.AnalyticsStore
import inc.uberpopug.analytics.service.{AnalyticsConsumer, AnalyticsService}

/** Точка входа aTES Analytics Service: собирает ZLayer-граф, поднимает HTTP-сервер и consumer событий.
  */
object Main extends ZIOAppDefault:
  /** Выделяет секцию `kafka` из AppConfig как отдельный слой для consumer-а. */
  private val kafkaConfig: ZLayer[AppConfig, Nothing, KafkaConfig] =
    ZLayer.fromFunction((cfg: AppConfig) => cfg.kafka)

  /** Единый Kafka-producer: публикация в DLQ. */
  private val producerLayer: ZLayer[AppConfig, Throwable, Producer] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[AppConfig]
        producer <- Producer.make(ProducerSettings(cfg.kafka.bootstrapServers).withClientId("ates-analytics-producer"))
      yield producer
    }

  /** Период опроса metric registry для Prometheus. */
  private val metricsConfig: ZLayer[Any, Nothing, MetricsConfig] =
    ZLayer.succeed(MetricsConfig(Duration.ofSeconds(10)))

  /** Единый граф зависимостей приложения: конфиг, пул соединений, Quill-контекст, репозитории, сервисы и tapir-server
    * logic. Собирается только здесь (SSOT композиции).
    */
  private val appLayer: ZLayer[
    Any,
    Throwable,
    AppConfig & DataSource & AnalyticsServerLogic & AnalyticsConsumer & PrometheusPublisher
  ] =
    ZLayer.make[
      AppConfig & DataSource & AnalyticsServerLogic & AnalyticsConsumer & PrometheusPublisher
    ](
      AppConfig.layer,
      DataSourceLayer.live,
      DbContext.live,
      AnalyticsStore.layer,
      AnalyticsService.layer,
      AnalyticsServerLogic.layer,
      AnalyticsConsumer.consumerLayer,
      producerLayer,
      kafkaConfig,
      AnalyticsConsumer.layer,
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
      logic <- ZIO.service[AnalyticsServerLogic]
      consumer <- ZIO.service[AnalyticsConsumer]
      prometheus <- ZIO.service[PrometheusPublisher]
      _ <- ZIO.logInfo(s"Starting aTES Analytics Service on port ${cfg.server.port}")
      metricsRoute = Routes(
        Method.GET / "metrics" -> handler(prometheus.get.map(text => Response.text(text)))
      )
      httpApp = ZioHttpInterpreter().toHttp(logic.endpoints) ++ metricsRoute
      _ <- consumer.run.fork
    yield Server
      .serve(httpApp)
      .provide(Server.defaultWithPort(cfg.server.port), ZLayer.succeed(Clock.ClockLive))).flatten.provideLayer(appLayer)
