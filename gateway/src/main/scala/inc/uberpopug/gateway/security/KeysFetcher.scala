package inc.uberpopug.gateway.security

import zio.{ZIO, ZLayer}
import zio.http.{Client, Request}
import zio.json.*

import inc.uberpopug.gateway.config.GatewayJwtConfig

/** Источник публичных JWK Auth: абстракция, чтобы KeyManager не зависел от HTTP напрямую (тестируемость). */
trait KeysFetcher:
  /** Возвращает список публичных ключей Auth. */
  def fetch: ZIO[Any, Throwable, JwksResponse]

object KeysFetcher:
  /** HTTP-реализация: `GET /auth/keys` через zio-http Client, тело парсится как `JwksResponse`. */
  private final case class HttpKeysFetcher(config: GatewayJwtConfig, client: Client) extends KeysFetcher:
    def fetch: ZIO[Any, Throwable, JwksResponse] =
      client
        .batched(Request.get(config.keysUrl))
        .flatMap { response =>
          if response.status.isSuccess then
            response.body.asString.flatMap(body => ZIO.fromEither(body.fromJson[JwksResponse]).mapError(errorMessage))
          else ZIO.fail(new RuntimeException(s"Auth keys request failed with status ${response.status}"))
        }

    private def errorMessage(parseError: String): Throwable =
      new RuntimeException(s"Invalid JWKS payload from ${config.keysUrl}: $parseError")

  /** Слой HTTP-реализации поверх `Client` и `GatewayJwtConfig`. */
  val layer: ZLayer[GatewayJwtConfig & Client, Nothing, KeysFetcher] =
    ZLayer.fromFunction(HttpKeysFetcher(_, _))
