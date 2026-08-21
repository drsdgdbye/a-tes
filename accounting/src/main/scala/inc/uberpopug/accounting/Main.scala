package inc.uberpopug.accounting

import java.time.Duration

import javax.sql.DataSource

import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.{Clock, ZIO, ZIOAppDefault, ZLayer}
import zio.http.{handler, Method, Response, Routes, Server}
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.prometheus
import zio.metrics.connectors.prometheus.PrometheusPublisher

import inc.uberpopug.accounting.api.AccountingServerLogic
import inc.uberpopug.accounting.config.{AppConfig, KafkaConfig}
import inc.uberpopug.accounting.db.{DataSourceLayer, DbContext, Migrations}
import inc.uberpopug.accounting.repository.{EventStore, OutboxRepository}
import inc.uberpopug.accounting.service.{
  AccountingConsumer,
  AccountingService,
  OutboxRelay,
  PayoutScheduler,
  PayoutService
}

/** Точка входа aTES Accounting Service: собирает ZLayer-граф, поднимает HTTP-сервер, outbox-relay, consumer событий и
  * планировщик выплат.
  */
object Main extends ZIOAppDefault:
  /** Выделяет секцию `kafka` из AppConfig как отдельный слой для consumer-а. */
  private val kafkaConfig: ZLayer[AppConfig, Nothing, KafkaConfig] =
    ZLayer.fromFunction((cfg: AppConfig) => cfg.kafka)

  /** Единый Kafka-producer: публикация DLQ и событий из outbox. */
  private val producerLayer: ZLayer[AppConfig, Throwable, Producer] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[AppConfig]
        producer <- Producer.make(ProducerSettings(cfg.kafka.bootstrapServers).withClientId("ates-accounting-producer"))
      yield producer
    }

  /** Период опроса metric registry для Prometheus. */
  private val metricsConfig: ZLayer[Any, Nothing, MetricsConfig] =
    ZLayer.succeed(MetricsConfig(Duration.ofSeconds(10)))

  /** Бесконечный цикл публикации gauge'ов пула HikariCP из MXBean. */
  private def hikariGaugeLoop(ds: javax.sql.DataSource): ZIO[Any, Nothing, Unit] =
    val publish =
      ZIO
        .attempt {
          val pool = ds.asInstanceOf[com.zaxxer.hikari.HikariDataSource].getHikariPoolMXBean
          zio.metrics.Metric.gauge("hikaricp_active_connections").set(pool.getActiveConnections.toDouble) *>
            zio.metrics.Metric.gauge("hikaricp_idle_connections").set(pool.getIdleConnections.toDouble) *>
            zio.metrics.Metric.gauge("hikaricp_total_connections").set(pool.getTotalConnections.toDouble) *>
            zio.metrics.Metric.gauge("hikaricp_awaiting_connections").set(pool.getThreadsAwaitingConnection.toDouble)
        }
        .flatten
        .catchAll(_ => ZIO.unit)
    (publish *> ZIO.sleep(Duration.ofSeconds(10))).forever

  /** Единый граф зависимостей приложения: конфиг, пул соединений, Quill-контекст, репозитории, сервисы и tapir-server
    * logic. Собирается только здесь (SSOT композиции).
    */
  private val appLayer: ZLayer[
    Any,
    Throwable,
    AppConfig & DataSource & AccountingServerLogic & OutboxRelay & AccountingConsumer & PayoutScheduler &
      PrometheusPublisher
  ] =
    ZLayer.make[
      AppConfig & DataSource & AccountingServerLogic & OutboxRelay & AccountingConsumer & PayoutScheduler &
        PrometheusPublisher
    ](
      AppConfig.layer,
      DataSourceLayer.live,
      DbContext.live,
      EventStore.layer,
      OutboxRepository.layer,
      AccountingService.layer,
      AccountingServerLogic.layer,
      AccountingConsumer.consumerLayer,
      producerLayer,
      kafkaConfig,
      AccountingConsumer.layer,
      OutboxRelay.layer,
      PayoutService.layer,
      PayoutScheduler.layer,
      prometheus.publisherLayer,
      prometheus.prometheusLayer,
      metricsConfig,
      ZLayer.succeed(Clock.ClockLive)
    )

  /** Стартовая последовательность: миграции БД, запуск outbox-relay, consumer-а и планировщика выплат в отдельных
    * волокнах и HTTP-сервер на порту из конфига.
    */
  override def run: ZIO[Any, Throwable, Nothing] =
    (for
      cfg <- ZIO.service[AppConfig]
      ds <- ZIO.service[DataSource]
      _ <- Migrations.migrate(ds)
      logic <- ZIO.service[AccountingServerLogic]
      relay <- ZIO.service[OutboxRelay]
      consumer <- ZIO.service[AccountingConsumer]
      scheduler <- ZIO.service[PayoutScheduler]
      prometheus <- ZIO.service[PrometheusPublisher]
      _ <- ZIO.logInfo(s"Starting aTES Accounting Service on port ${cfg.server.port}")
      metricsRoute = Routes(
        Method.GET / "metrics" -> handler(prometheus.get.map(text => Response.text(text)))
      )
      httpApp = ZioHttpInterpreter().toHttp(logic.endpoints) ++ metricsRoute
      _ <- relay.run.fork
      _ <- consumer.run.fork
      _ <- scheduler.run.fork
      _ <- hikariGaugeLoop(ds).fork
    yield Server
      .serve(httpApp)
      .provide(Server.defaultWithPort(cfg.server.port), ZLayer.succeed(Clock.ClockLive))).flatten.provideLayer(appLayer)
