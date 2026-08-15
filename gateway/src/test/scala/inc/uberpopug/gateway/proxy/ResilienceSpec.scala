package inc.uberpopug.gateway.proxy

import zio.*
import zio.http.Method
import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.gateway.config.*

object ResilienceSpec extends ZIOSpecDefault:
  private val config = ResilienceConfig(
    circuitBreaker = CircuitBreakerConfig(maxFailures = 5, resetIntervalSeconds = 30L),
    retry = RetryConfig(maxRetries = 3, initialDelayMillis = 100L, backoffFactor = 2.0),
    timeLimiter = TimeLimiterConfig(readTimeoutSeconds = 5L, writeTimeoutSeconds = 10L),
    bulkhead = BulkheadConfig(maxConcurrent = 20),
    rateLimiter = RateLimiterConfig(rps = 200)
  )

  private def policies(): ZIO[Resilience, Nothing, ServicePolicies] =
    ZIO.service[Resilience].map(_.forDownstream(Downstream.TaskService))

  /** Продвигает TestClock шагами, пока `condition` не выполнится или не завершится `fiber`. */
  private def driveUntil(condition: UIO[Boolean]): UIO[Unit] =
    condition.flatMap {
      case true  => ZIO.unit
      case false => TestClock.adjust(100.millis) *> driveUntil(condition)
    }

  def spec: Spec[Any, Any] =
    suite("Resilience")(
      test("retries transient errors and eventually succeeds") {
        for
          counter <- Ref.make(0)
          task = counter.getAndUpdate(_ + 1).flatMap { n =>
            if n <= 2 then ZIO.fail(GatewayError.DownstreamUnreachable("down"))
            else ZIO.succeed(n)
          }
          policies <- policies()
          fiber <- policies.protect(Method.GET, task).fork
          _ <- driveUntil(counter.get.map(_ >= 4))
          result <- fiber.join
        yield assertTrue(result == 3)
      },
      test("gives up on persistent failures after retries") {
        val failing: ZIO[Any, GatewayError, Unit] = ZIO.fail(GatewayError.DownstreamUnreachable("down"))
        for
          counter <- Ref.make(0)
          attempts = counter.getAndUpdate(_ + 1) *> failing
          policies <- policies()
          fiber <- policies.protect(Method.GET, attempts).either.fork
          _ <- driveUntil(counter.get.map(_ >= 4))
          result <- fiber.join
        yield assertTrue(result.isLeft)
      },
      test("times out calls exceeding the read timeout") {
        for
          policies <- policies()
          fiber <- policies.protect(Method.GET, ZIO.never).fork
          _ <- driveUntil(fiber.poll.map(_.isDefined))
          result <- fiber.join.either
        yield assert(result)(isLeft(equalTo(GatewayError.CallTimedOut)))
      },
      test("write calls get the longer write timeout") {
        for
          policies <- policies()
          fiber <- policies.protect(Method.POST, ZIO.unit.delay(7.seconds)).fork
          _ <- driveUntil(fiber.poll.map(_.isDefined))
          result <- fiber.join
        yield assertTrue(result == ())
      },
      test("circuit breaker opens after maxFailures and fails fast") {
        val failing: ZIO[Any, GatewayError, Unit] = ZIO.fail(GatewayError.DownstreamUnreachable("down"))
        for
          attempts <- Ref.make(0)
          call = attempts.update(_ + 1) *> failing
          policies <- policies()
          fiber <- (policies.protect(Method.GET, call).either *> ZIO.unit).replicateZIO(5).fork
          _ <- driveUntil(attempts.get.map(_ >= 4 * 5))
          _ <- fiber.join
          opened <- policies.protect(Method.GET, call).either
        yield assert(opened)(isLeft(equalTo(GatewayError.DownstreamUnreachable("Circuit breaker open"))))
      }
    ).provideLayer(ZLayer.succeed(config) >>> Resilience.layer)
