package inc.uberpopug.gateway.proxy

import nl.vroste.rezilience.{Bulkhead, CircuitBreaker, RateLimiter, Retry, Timeout}
import zio.{Duration, Schedule, Scope, ZIO, ZLayer}
import zio.http.Method
import zio.metrics.MetricLabel

import inc.uberpopug.gateway.config.{CircuitBreakerConfig, ResilienceConfig, RetryConfig}

/** Набор resilience-политик (rezilience) для одного downstream-сервиса. */
final case class ServicePolicies(
    circuitBreaker: CircuitBreaker[GatewayError],
    retry: Retry[GatewayError],
    readTimeout: Timeout,
    writeTimeout: Timeout,
    bulkhead: Bulkhead,
    rateLimiter: RateLimiter
):
  /** Выполняет задачу под всеми политиками: RateLimiter → Bulkhead → CircuitBreaker → Retry → Timeout (таймаут по
    * методу: read 5s / write 10s).
    */
  def protect[Env, A](method: Method, task: ZIO[Env, GatewayError, A]): ZIO[Env, GatewayError, A] =
    val timeout = if isReadMethod(method) then readTimeout else writeTimeout
    timeout(bulkhead(rateLimiter(circuitBreaker(retry(task))))).mapError(unwrap)

  /** Разворачивает вложенные ошибки политик в единый `GatewayError`. */
  private def unwrap(
      error: Timeout.TimeoutError[Bulkhead.BulkheadError[CircuitBreaker.CircuitBreakerCallError[GatewayError]]]
  ): GatewayError =
    error match
      case Timeout.CallTimedOut                => GatewayError.CallTimedOut
      case Timeout.WrappedError(bulkheadError) =>
        bulkheadError match
          case Bulkhead.BulkheadRejection                 => GatewayError.DownstreamUnreachable("Bulkhead queue full")
          case Bulkhead.WrappedError(circuitBreakerError) =>
            circuitBreakerError match
              case CircuitBreaker.CircuitBreakerOpen => GatewayError.DownstreamUnreachable("Circuit breaker open")
              case CircuitBreaker.WrappedError(gatewayError) => gatewayError

  private def isReadMethod(method: Method): Boolean =
    method == Method.GET || method == Method.HEAD || method == Method.OPTIONS

/** Resilience-политики для всех downstream-сервисов. */
final case class Resilience(
    auth: ServicePolicies,
    taskService: ServicePolicies,
    accounting: ServicePolicies,
    analytics: ServicePolicies
):
  /** Политики для конкретного downstream-сервиса. */
  def forDownstream(downstream: Downstream): ServicePolicies = downstream match
    case Downstream.Auth        => auth
    case Downstream.TaskService => taskService
    case Downstream.Accounting  => accounting
    case Downstream.Analytics   => analytics

object Resilience:
  /** Слой: Scoped-политики для каждого из четырёх downstream-сервисов на время жизни приложения. */
  val layer: ZLayer[ResilienceConfig, Nothing, Resilience] =
    ZLayer.scoped {
      for
        config <- ZIO.service[ResilienceConfig]
        auth <- makePolicies(config, "auth")
        taskService <- makePolicies(config, "task-service")
        accounting <- makePolicies(config, "accounting")
        analytics <- makePolicies(config, "analytics")
      yield Resilience(auth, taskService, accounting, analytics)
    }

  private def makePolicies(config: ResilienceConfig, serviceName: String): ZIO[Scope, Nothing, ServicePolicies] =
    for
      circuitBreaker <- makeCircuitBreaker(config.circuitBreaker, serviceName)
      retry <- Retry.make[Any, GatewayError](retrySchedule(config.retry))
      readTimeout <- Timeout.make(Duration.fromSeconds(config.timeLimiter.readTimeoutSeconds))
      writeTimeout <- Timeout.make(Duration.fromSeconds(config.timeLimiter.writeTimeoutSeconds))
      bulkhead <- Bulkhead.make(config.bulkhead.maxConcurrent)
      rateLimiter <- RateLimiter.make(config.rateLimiter.rps)
    yield ServicePolicies(circuitBreaker, retry, readTimeout, writeTimeout, bulkhead, rateLimiter)

  private def makeCircuitBreaker(
      config: CircuitBreakerConfig,
      serviceName: String
  ): ZIO[Scope, Nothing, CircuitBreaker[GatewayError]] =
    CircuitBreaker.withMaxFailures[GatewayError](
      maxFailures = config.maxFailures,
      resetPolicy = Schedule.fixed(Duration.fromSeconds(config.resetIntervalSeconds)),
      metricLabels = Some(Set(MetricLabel("service", serviceName)))
    )

  /** Retry только на транзиентных ошибках: exponential backoff с ограничением числа попыток. */
  private def retrySchedule(config: RetryConfig): Schedule[Any, GatewayError, Any] =
    val minDelay = Duration.fromMillis(config.initialDelayMillis)
    val maxDelay =
      Duration.fromMillis((config.initialDelayMillis * math.pow(config.backoffFactor, config.maxRetries)).toLong)
    Retry.Schedules.whenCase[Any, GatewayError, (Any, Long)](transientErrors) {
      Retry.Schedules.common(
        min = minDelay,
        max = maxDelay,
        factor = config.backoffFactor,
        maxRetries = Some(config.maxRetries)
      )
    }

  /** Транзиентные ошибки, которые стоит повторить: недоступность и retryable 5xx downstream. */
  private val transientErrors: PartialFunction[GatewayError, Any] = {
    case GatewayError.DownstreamUnreachable(_) => ()
    case GatewayError.DownstreamRetryable(_)   => ()
  }
