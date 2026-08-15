package inc.uberpopug.gateway.security

import zio.{Duration, Ref, Schedule, ZIO, ZLayer}

import inc.uberpopug.gateway.config.GatewayJwtConfig

/** Управление публичным ключом Auth: стартовый fetch с бесконечным retry и периодический refresh. Ключ Auth эфемерный —
  * пересоздаётся при каждом рестарте Auth, поэтому Gateway обязан обновлять его периодически.
  */
final case class KeyManager(config: GatewayJwtConfig, fetcher: KeysFetcher, verifierRef: Ref[Option[JwtVerifier]]):

  /** Текущий верификатор; `None` только пока стартовый fetch не завершился. */
  def verifier: ZIO[Any, Nothing, Option[JwtVerifier]] = verifierRef.get

  /** Готов ли Gateway верифицировать токены (readiness). */
  def isReady: ZIO[Any, Nothing, Boolean] = verifierRef.get.map(_.isDefined)

  /** Стартовый fetch: повторяется бесконечно, пока Auth недоступен (Auth критичен — Gateway не стартует без ключа). */
  def startup: ZIO[Any, Nothing, Unit] =
    refreshOnce
      .tapError(e => ZIO.logError(s"Failed to fetch Auth JWT keys: ${e.getMessage}; retrying"))
      .retry(Schedule.fixed(Duration.fromSeconds(config.startupRetryIntervalSeconds)))
      .orDie

  /** Периодический refresh ключа; ошибка логируется и сохраняется прежний ключ. */
  def refreshLoop: ZIO[Any, Nothing, Unit] =
    refreshOnce
      .tapError(e => ZIO.logWarning(s"JWT keys refresh failed, keeping current verifier: ${e.getMessage}"))
      .ignore
      .repeat(Schedule.fixed(Duration.fromSeconds(config.refreshIntervalSeconds)))
      .unit

  private def refreshOnce: ZIO[Any, Throwable, Unit] =
    fetcher.fetch
      .flatMap(jwks => ZIO.fromEither(JwtVerifier.fromJwks(jwks, config.issuer)).mapError(e => new RuntimeException(e)))
      .flatMap { verifier =>
        verifierRef.set(Some(verifier)) *> ZIO.logInfo(s"JWT verifier loaded: ${verifier.keyId}")
      }

object KeyManager:
  /** Слой: стартовый fetch (до загрузки ключа) и фоновый refresh на всё время жизни приложения. */
  val layer: ZLayer[GatewayJwtConfig & KeysFetcher, Nothing, KeyManager] =
    ZLayer.scoped {
      for
        config <- ZIO.service[GatewayJwtConfig]
        fetcher <- ZIO.service[KeysFetcher]
        ref <- Ref.make(Option.empty[JwtVerifier])
        manager = KeyManager(config, fetcher, ref)
        _ <- manager.startup
        _ <- manager.refreshLoop.fork
      yield manager
    }
