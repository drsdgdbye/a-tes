package inc.uberpopug.auth

import javax.sql.DataSource

import sttp.tapir.server.ziohttp.ZioHttpInterpreter
import zio.{Clock, ZIO, ZIOAppDefault, ZLayer}
import zio.http.Server

import inc.uberpopug.auth.api.AuthServerLogic
import inc.uberpopug.auth.config.{AppConfig, AuthConfig, JwtConfig}
import inc.uberpopug.auth.db.{DataSourceLayer, DbContext, Migrations}
import inc.uberpopug.auth.repository.{OutboxRepository, RefreshTokenRepository, UserRepository}
import inc.uberpopug.auth.service.{AuthService, OutboxRelay, PasswordHasher, TokenService}

/** Точка входа aTES Auth Service: собирает ZLayer-граф и поднимает HTTP-сервер. */
object Main extends ZIOAppDefault:
  /** Выделяет секцию `jwt` из AppConfig как отдельный слой для TokenService. */
  private val jwtConfig: ZLayer[AppConfig, Nothing, JwtConfig] =
    ZLayer.fromFunction((cfg: AppConfig) => cfg.jwt)

  /** Выделяет секцию `auth` из AppConfig как отдельный слой для AuthService и server logic. */
  private val authConfig: ZLayer[AppConfig, Nothing, AuthConfig] =
    ZLayer.fromFunction((cfg: AppConfig) => cfg.auth)

  /** Единый граф зависимостей приложения: конфиг, пул соединений, Quill-контекст, репозитории, сервисы и tapir-server
    * logic. Собирается только здесь (SSOT композиции).
    */
  private val appLayer: ZLayer[Any, Throwable, AppConfig & DataSource & AuthServerLogic & OutboxRelay] =
    ZLayer.make[AppConfig & DataSource & AuthServerLogic & OutboxRelay](
      AppConfig.layer,
      DataSourceLayer.live,
      DbContext.live,
      UserRepository.layer,
      OutboxRepository.layer,
      RefreshTokenRepository.layer,
      PasswordHasher.layer,
      jwtConfig,
      authConfig,
      TokenService.layer,
      AuthService.layer,
      AuthServerLogic.layer,
      OutboxRelay.producerLayer,
      OutboxRelay.layer,
      ZLayer.succeed(Clock.ClockLive)
    )

  /** Стартовая последовательность: миграции БД, запуск outbox-relay в отдельном волокне и HTTP-сервер на порту из
    * конфига.
    */
  override def run: ZIO[Any, Throwable, Nothing] =
    (for
      cfg <- ZIO.service[AppConfig]
      ds <- ZIO.service[DataSource]
      _ <- Migrations.migrate(ds)
      logic <- ZIO.service[AuthServerLogic]
      relay <- ZIO.service[OutboxRelay]
      _ <- ZIO.logInfo(s"Starting aTES Auth Service on port ${cfg.server.port}")
      httpApp = ZioHttpInterpreter().toHttp(logic.endpoints)
      _ <- relay.run.fork
    yield Server
      .serve(httpApp)
      .provide(Server.defaultWithPort(cfg.server.port), ZLayer.succeed(Clock.ClockLive))).flatten.provideLayer(appLayer)
