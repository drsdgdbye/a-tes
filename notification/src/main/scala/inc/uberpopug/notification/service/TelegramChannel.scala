package inc.uberpopug.notification.service

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets

import nl.vroste.rezilience.{CircuitBreaker, RateLimiter, Retry, Timeout}
import zio.{Duration, Schedule, Scope, ZIO}
import zio.json.{DecoderOps, DeriveJsonDecoder, DeriveJsonEncoder, EncoderOps, JsonDecoder, JsonEncoder}
import zio.metrics.MetricLabel

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.TelegramSendFailed
import inc.uberpopug.notification.config.TelegramChannelConfig
import inc.uberpopug.notification.domain.{ChannelType, RenderedMessage}

/** Запрос к Telegram Bot API `sendMessage`. */
final case class TelegramSendRequest(chatId: String, text: String)

object TelegramSendRequest:
  given JsonEncoder[TelegramSendRequest] = DeriveJsonEncoder.gen

/** Ответ Telegram Bot API: `{ ok: bool, description?: string }`. */
final case class TelegramApiResponse(ok: Boolean, description: Option[String])

object TelegramApiResponse:
  given JsonDecoder[TelegramApiResponse] = DeriveJsonDecoder.gen

/** «Сырой» клиент Telegram Bot API: один HTTP-запрос, без resilience-политик. */
trait RawTelegramClient:
  def send(chatId: String, text: String): ZIO[Any, DomainError, Unit]

/** HTTP-реализация через `java.net.http.HttpClient` (JDK 21, без дополнительных зависимостей). */
final case class HttpTelegramClient(http: HttpClient, botToken: String) extends RawTelegramClient:
  def send(chatId: String, text: String): ZIO[Any, DomainError, Unit] =
    ZIO
      .attemptBlocking {
        val body = TelegramSendRequest(chatId, text).toJson.getBytes(StandardCharsets.UTF_8)
        val request = HttpRequest
          .newBuilder(URI.create(s"https://api.telegram.org/bot$botToken/sendMessage"))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofByteArray(body))
          .build()
        http.send(request, HttpResponse.BodyHandlers.ofString())
      }
      .mapError(ex => TelegramSendFailed(Option(ex.getMessage).getOrElse(ex.getClass.getSimpleName)))
      .flatMap { response =>
        if response.statusCode() == 200 then
          ZIO
            .fromEither(response.body().fromJson[TelegramApiResponse])
            .mapError(msg => TelegramSendFailed(s"Invalid Telegram API response: $msg"))
            .flatMap { api =>
              if api.ok then ZIO.unit
              else ZIO.fail(TelegramSendFailed(api.description.getOrElse("Unknown Telegram API error")))
            }
        else ZIO.fail(TelegramSendFailed(s"Telegram API HTTP ${response.statusCode()}: ${response.body().take(200)}"))
      }

/** Telegram-канал доставки с resilience-политиками (спека §7.2): TimeLimiter → RateLimiter → CircuitBreaker → Retry.
  * Политики scoped на время жизни приложения.
  */
final case class TelegramChannel(
    raw: RawTelegramClient,
    rateLimiter: RateLimiter,
    circuitBreaker: CircuitBreaker[DomainError],
    timeout: Timeout,
    retry: Retry[DomainError]
) extends NotificationChannel:
  val channelType: ChannelType = ChannelType.Telegram

  def send(message: RenderedMessage): ZIO[Any, DomainError, Unit] =
    timeout(rateLimiter(circuitBreaker(retry(raw.send(message.address, message.text))))).mapError(unwrap)

  /** Разворачивает вложенные ошибки политик в единый `TelegramSendFailed`. */
  private def unwrap(
      error: Timeout.TimeoutError[CircuitBreaker.CircuitBreakerCallError[DomainError]]
  ): DomainError =
    error match
      case Timeout.CallTimedOut          => TelegramSendFailed("Telegram API request timed out")
      case Timeout.WrappedError(cbError) =>
        cbError match
          case CircuitBreaker.CircuitBreakerOpen        => TelegramSendFailed("Telegram API circuit breaker is open")
          case CircuitBreaker.WrappedError(domainError) => domainError

object TelegramChannel:
  /** Собирает канал: сырой клиент + resilience-политики из конфига канала. */
  def make(config: TelegramChannelConfig): ZIO[Scope, Nothing, NotificationChannel] =
    for
      raw <- ZIO.succeed(HttpTelegramClient(HttpClient.newHttpClient(), config.botToken))
      circuitBreaker <- CircuitBreaker.withMaxFailures[DomainError](
        maxFailures = 5,
        resetPolicy = Schedule.fixed(Duration.fromSeconds(60)),
        metricLabels = Some(Set(MetricLabel("channel", "telegram")))
      )
      rateLimiter <- RateLimiter.make(config.rateLimitPerSecond)
      timeout <- Timeout.make(Duration.fromSeconds(config.sendTimeoutSeconds))
      retry <- Retry.make[Any, DomainError](retrySchedule(config.retryAttempts))
    yield TelegramChannel(raw, rateLimiter, circuitBreaker, timeout, retry)

  /** Retry на транзиентных ошибках доставки: exponential backoff, всего `attempts` попыток. */
  private def retrySchedule(attempts: Int): Schedule[Any, DomainError, Any] =
    val minDelay = Duration.fromMillis(100)
    val maxDelay = Duration.fromMillis((100 * math.pow(2.0, attempts.toDouble)).toLong)
    Retry.Schedules.whenCase[Any, DomainError, (Any, Long)]({ case TelegramSendFailed(_) => () }) {
      Retry.Schedules.common(
        min = minDelay,
        max = maxDelay,
        factor = 2.0,
        maxRetries = Some(attempts - 1)
      )
    }
