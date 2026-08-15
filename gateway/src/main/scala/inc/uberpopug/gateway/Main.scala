package inc.uberpopug.gateway

import java.time.Duration

import zio.{Clock, ZIO, ZIOAppDefault, ZLayer}
import zio.http.{Client, Server, ZClient}
import zio.metrics.connectors.MetricsConfig
import zio.metrics.connectors.prometheus

import inc.uberpopug.gateway.api.GatewayRoutes
import inc.uberpopug.gateway.config.{AppConfig, GatewayJwtConfig, ResilienceConfig, ServerConfig, ServicesConfig}
import inc.uberpopug.gateway.proxy.{DownstreamClient, Resilience}
import inc.uberpopug.gateway.security.{KeysFetcher, KeyManager}

/** Точка входа aTES API Gateway: собирает ZLayer-граф (конфиг, JWT-ключи Auth, resilience, проксирование) и поднимает
  * HTTP-сервер.
  */
object Main extends ZIOAppDefault:
  /** Выделяет секцию `server` из AppConfig. */
  private val serverConfig: ZLayer[AppConfig, Nothing, ServerConfig] =
    ZLayer.fromFunction((cfg: AppConfig) => cfg.server)

  /** Выделяет секцию `jwt` из AppConfig. */
  private val gatewayJwtConfig: ZLayer[AppConfig, Nothing, GatewayJwtConfig] =
    ZLayer.fromFunction((cfg: AppConfig) => cfg.jwt)

  /** Выделяет секцию `services` из AppConfig. */
  private val servicesConfig: ZLayer[AppConfig, Nothing, ServicesConfig] =
    ZLayer.fromFunction((cfg: AppConfig) => cfg.services)

  /** Выделяет секцию `resilience` из AppConfig. */
  private val resilienceConfig: ZLayer[AppConfig, Nothing, ResilienceConfig] =
    ZLayer.fromFunction((cfg: AppConfig) => cfg.resilience)

  /** HTTP-сервер на порту из конфига. */
  private val serverLayer: ZLayer[ServerConfig, Throwable, Server] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[ServerConfig]
        server <- Server.defaultWithPort(cfg.port).build.map(_.get)
      yield server
    }

  /** Период опроса metric registry для Prometheus. */
  private val metricsConfig: ZLayer[Any, Nothing, MetricsConfig] =
    ZLayer.succeed(MetricsConfig(Duration.ofSeconds(10)))

  /** Единый граф зависимостей Gateway: конфиг, HTTP-клиент, JWT-ключи Auth, resilience, проксирование, Prometheus и
    * маршруты. Собирается только здесь (SSOT композиции).
    */
  private val appLayer: ZLayer[Any, Throwable, GatewayRoutes & Server & Client & Clock] =
    ZLayer.make[GatewayRoutes & Server & Client & Clock](
      AppConfig.layer,
      serverConfig,
      gatewayJwtConfig,
      servicesConfig,
      resilienceConfig,
      serverLayer,
      ZClient.default,
      ZLayer.succeed(Clock.ClockLive),
      KeysFetcher.layer,
      KeyManager.layer,
      Resilience.layer,
      ZLayer.fromFunction(DownstreamClient.apply),
      prometheus.publisherLayer,
      prometheus.prometheusLayer,
      metricsConfig,
      ZLayer.fromFunction(GatewayRoutes.apply)
    )

  /** Стартовая последовательность: Gateway стартует после загрузки JWT-ключей (startup-retry в KeyManager.layer) и
    * обслуживает health/ready/metrics + reverse proxy.
    */
  override def run: ZIO[Any, Throwable, Nothing] =
    (
      for
        gateway <- ZIO.service[GatewayRoutes]
        _ <- ZIO.logInfo("Starting aTES API Gateway")
        port <- ZIO.serviceWithZIO[Server](_.port)
        _ <- ZIO.logInfo(s"Gateway listening on :$port")
      yield gateway
    ).flatMap(gateway => Server.serve(gateway.routes)).provideLayer(appLayer)
