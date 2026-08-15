package inc.uberpopug.gateway.api

import java.util.Date

import zio.*
import zio.http.*
import zio.metrics.connectors.prometheus.PrometheusPublisher
import zio.test.*

import inc.uberpopug.gateway.config.*
import inc.uberpopug.gateway.proxy.*
import inc.uberpopug.gateway.security.*

object GatewayProxySpec extends ZIOSpecDefault:
  private val issuer = "ates-auth"
  private val jwt = TestJwt.make(issuer)
  private val jwtConfig =
    GatewayJwtConfig(issuer, "http://auth/keys", refreshIntervalSeconds = 60L, startupRetryIntervalSeconds = 2L)
  private val resilienceConfig = ResilienceConfig(
    circuitBreaker = CircuitBreakerConfig(5, 30L),
    retry = RetryConfig(3, 100L, 2.0),
    timeLimiter = TimeLimiterConfig(5L, 10L),
    bulkhead = BulkheadConfig(20),
    rateLimiter = RateLimiterConfig(200)
  )
  private val subject = "550e8400-e29b-41d4-a716-446655440000"
  private val future = new Date(java.lang.System.currentTimeMillis() + 3600_000)

  private final class UnusedFetcher extends KeysFetcher:
    def fetch: ZIO[Any, Throwable, JwksResponse] = ZIO.succeed(jwt.jwks)

  /** Mock downstream: публичный путь Auth и защищённый путь задач на одном сервере. */
  private val mockDownstream: Routes[Any, Response] =
    Routes(
      Method.GET / "auth" / "login" -> handler(Response.json("""{"ok":true}""")),
      Method.GET / "tasks" -> handler(Response.json("""{"tasks":[]}"""))
    )

  private def servicesAt(port: Int): ServicesConfig =
    val base = ServiceConfig(s"http://localhost:$port")
    ServicesConfig(auth = base, taskService = base, accounting = base, analytics = base)

  /** Собирает GatewayRoutes поверх реального mock-сервера и прогоняет callback. */
  private def withGateway[A](
      f: (Int, Routes[Any, Nothing]) => ZIO[Scope, Throwable, A]
  ): ZIO[Server & Client & Resilience & Scope, Throwable, A] =
    for
      port <- Server.install(mockDownstream)
      client <- ZIO.service[Client]
      resilience <- ZIO.service[Resilience]
      prometheus <- PrometheusPublisher.make
      ref <- Ref.make[Option[JwtVerifier]](Some(jwt.verifier))
      keyManager = KeyManager(jwtConfig, new UnusedFetcher, ref)
      gateway = GatewayRoutes(
        keyManager,
        resilience,
        DownstreamClient(servicesAt(port)),
        prometheus,
        client,
        Clock.ClockLive
      ).routes
      result <- f(port, gateway)
    yield result

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("GatewayProxy")(
      test("health returns 200") {
        withGateway { (_, routes) =>
          routes
            .runZIO(Request.get("http://localhost/health"))
            .map(response => assertTrue(response.status == Status.Ok))
        }
      },
      test("ready returns 200 when JWT keys are loaded") {
        withGateway { (_, routes) =>
          routes.runZIO(Request.get("http://localhost/ready")).map(response => assertTrue(response.status == Status.Ok))
        }
      },
      test("proxies a public auth path without a JWT and passes the body through") {
        withGateway { (_, routes) =>
          routes.runZIO(Request.get("http://localhost/auth/login")).flatMap { response =>
            response.body.asString.map(body => assertTrue(response.status == Status.Ok, body == """{"ok":true}"""))
          }
        }
      },
      test("proxies a protected path with a valid token") {
        val token = jwt.sign(subject, issuer, "manager", future)
        withGateway { (_, routes) =>
          routes
            .runZIO(Request.get("http://localhost/tasks").addHeader(Header.Authorization.Bearer(token)))
            .map(response => assertTrue(response.status == Status.Ok))
        }
      },
      test("rejects a protected path without a token") {
        withGateway { (_, routes) =>
          routes.runZIO(Request.get("http://localhost/tasks")).flatMap { response =>
            response.body.asString
              .map(body => assertTrue(response.status == Status.Unauthorized, body.contains("unauthorized")))
          }
        }
      },
      test("rejects a protected path with an invalid token") {
        val token = TestJwt.make(issuer).sign(subject, issuer, "manager", future)
        withGateway { (_, routes) =>
          routes
            .runZIO(Request.get("http://localhost/tasks").addHeader(Header.Authorization.Bearer(token)))
            .map(response => assertTrue(response.status == Status.Unauthorized))
        }
      },
      test("returns 404 for an unknown route") {
        withGateway { (_, routes) =>
          routes
            .runZIO(Request.get("http://localhost/nonexistent"))
            .map(response => assertTrue(response.status == Status.NotFound))
        }
      }
    ).provideSomeLayer[TestEnvironment & Scope](
      Server.defaultWithPort(0) ++ ZClient.default ++ (ZLayer.succeed(resilienceConfig) >>> Resilience.layer)
    )
