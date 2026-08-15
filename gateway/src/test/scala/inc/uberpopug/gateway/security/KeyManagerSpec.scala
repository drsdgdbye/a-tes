package inc.uberpopug.gateway.security

import java.util.concurrent.atomic.AtomicInteger

import zio.*
import zio.durationInt
import zio.test.*

import inc.uberpopug.gateway.config.GatewayJwtConfig

object KeyManagerSpec extends ZIOSpecDefault:
  private val issuer = "ates-auth"
  private val config =
    GatewayJwtConfig(issuer, "http://auth/keys", refreshIntervalSeconds = 60L, startupRetryIntervalSeconds = 2L)
  private val first = TestJwt.make(issuer, kid = "first-kid")
  private val second = TestJwt.make(issuer, kid = "second-kid")

  /** Фетчер по сценарию: первый N вызовов по списку; дальнейшие повторяют последний. Индексация ленивая — каждый повтор
    * retry/refresh перезапускает эффект заново.
    */
  private final class ScriptedFetcher(script: List[Either[Throwable, JwksResponse]]) extends KeysFetcher:
    val calls: AtomicInteger = new AtomicInteger(0)
    def fetch: ZIO[Any, Throwable, JwksResponse] =
      ZIO
        .succeed(math.min(calls.getAndIncrement(), script.size - 1))
        .flatMap(index => ZIO.fromEither(script(index)))

  private def makeManager(fetcher: KeysFetcher): UIO[KeyManager] =
    Ref.make(Option.empty[JwtVerifier]).map(ref => KeyManager(config, fetcher, ref))

  /** Ждёт, пока фетчер сделает не менее `atLeast` вызовов, продвигая TestClock шагами (двигает фоновый refreshLoop). */
  private def advanceUntil(fetcher: ScriptedFetcher, atLeast: Int): UIO[Unit] =
    ZIO.succeed(fetcher.calls.get() >= atLeast).flatMap {
      case true  => ZIO.unit
      case false => TestClock.adjust(100.millis) *> advanceUntil(fetcher, atLeast)
    }

  def spec: Spec[Any, Any] =
    suite("KeyManager")(
      test("startup loads the verifier when Auth is reachable") {
        for
          manager <- makeManager(new ScriptedFetcher(List(Right(first.jwks))))
          _ <- manager.startup
          ready <- manager.isReady
          verifier <- manager.verifier
        yield assertTrue(ready, verifier.exists(_.keyId == "first-kid"))
      },
      test("startup retries until Auth responds") {
        val fetcher = new ScriptedFetcher(List(Left(new RuntimeException("auth down")), Right(first.jwks)))
        for
          manager <- makeManager(fetcher)
          fiber <- manager.startup.fork
          _ <- advanceUntil(fetcher, 2)
          _ <- fiber.join
          ready <- manager.isReady
        yield assertTrue(ready)
      },
      test("refreshLoop replaces the verifier when the key rotates") {
        val fetcher = new ScriptedFetcher(List(Right(first.jwks), Right(second.jwks)))
        for
          manager <- makeManager(fetcher)
          _ <- manager.startup
          fiber <- manager.refreshLoop.fork
          _ <- advanceUntil(fetcher, 2)
          verifier <- manager.verifier
          _ <- fiber.interrupt
        yield assertTrue(verifier.exists(_.keyId == "second-kid"))
      },
      test("refresh failure keeps the current verifier, then loads the next key") {
        val fetcher =
          new ScriptedFetcher(List(Right(first.jwks), Left(new RuntimeException("boom")), Right(second.jwks)))
        for
          manager <- makeManager(fetcher)
          _ <- manager.startup
          fiber <- manager.refreshLoop.fork
          _ <- advanceUntil(fetcher, 2)
          afterFailure <- manager.verifier
          _ <- advanceUntil(fetcher, 3)
          afterRecovery <- manager.verifier
          _ <- fiber.interrupt
        yield assertTrue(
          afterFailure.exists(_.keyId == "first-kid"),
          afterRecovery.exists(_.keyId == "second-kid")
        )
      }
    )
