package inc.uberpopug.notification.service

import nl.vroste.rezilience.{CircuitBreaker, RateLimiter, Retry, Timeout}
import zio.{Duration, Ref, Schedule, Scope, ZIO, durationInt}
import zio.test.*
import zio.test.Assertion.*
import zio.test.Live

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.TelegramSendFailed
import inc.uberpopug.notification.domain.{ChannelType, RenderedMessage}

/** Тесты Telegram-канала с resilience-политиками (M-NTF-08..11): retry, CircuitBreaker, RateLimiter, TimeLimiter.
  * Telegram API не вызывается — вместо него mock `RawTelegramClient`; политики rezilience собираются с тестовыми
  * параметрами (небольшие тайминги вместо 10s/60s из проды).
  *
  * Важно: политики — scoped-ресурсы (RateLimiter поднимает поток очереди, CB — таймер сброса). Канал и отправки живут в
  * одном `ZIO.scoped`, чтобы scope не закрылся раньше вызова `send` (иначе очередь RateLimiter остаётся без потребителя
  * и `send` зависает). Тесты выполняются под `Live.live`: тайминги rezilience должны идти по реальным часам, а не по
  * TestClock (который в тестах не продвигается сам).
  */
object TelegramChannelSpec extends ZIOSpecDefault:

  private val message = RenderedMessage(ChannelType.Telegram, "chat-1", "text")

  /** Mock клиента: успех. */
  private def succeedRaw: RawTelegramClient =
    new RawTelegramClient:
      def send(chatId: String, text: String): ZIO[Any, DomainError, Unit] = ZIO.unit

  /** Mock клиента: всегда падает, считает вызовы (для проверки числа retry-попыток). */
  private def failingRaw(calls: Ref[Int]): RawTelegramClient =
    new RawTelegramClient:
      def send(chatId: String, text: String): ZIO[Any, DomainError, Unit] =
        calls.update(_ + 1) *> ZIO.fail(TelegramSendFailed("telegram down"))

  /** Mock клиента: медленный (спит), для проверки TimeLimiter. */
  private def slowRaw(millis: Long): RawTelegramClient =
    new RawTelegramClient:
      def send(chatId: String, text: String): ZIO[Any, DomainError, Unit] =
        ZIO.sleep(Duration.fromMillis(millis)) *> ZIO.unit

  /** Mock клиента: считает параллельные вызовы в моменте (для проверки RateLimiter). */
  private def concurrentRaw(sleepMillis: Long, inFlight: Ref[Int], maxConcurrent: Ref[Int]): RawTelegramClient =
    new RawTelegramClient:
      def send(chatId: String, text: String): ZIO[Any, DomainError, Unit] =
        for
          _ <- inFlight.update(_ + 1)
          current <- inFlight.get
          _ <- maxConcurrent.update(m => math.max(m, current))
          _ <- ZIO.sleep(Duration.fromMillis(sleepMillis))
          _ <- inFlight.update(_ - 1)
        yield ()

  /** Собирает канал из mock-клиента и политик rezilience с тестовыми параметрами. */
  private def buildChannel(
      raw: RawTelegramClient,
      cbMaxFailures: Int = 1000,
      ratePerSecond: Int = 1000,
      sendTimeoutSeconds: Int = 60,
      retryMaxRetries: Int = 0
  ): ZIO[Scope, Nothing, TelegramChannel] =
    for
      circuitBreaker <- CircuitBreaker.withMaxFailures[DomainError](
        maxFailures = cbMaxFailures,
        resetPolicy = Schedule.fixed(Duration.fromSeconds(60))
      )
      rateLimiter <- RateLimiter.make(ratePerSecond)
      timeout <- Timeout.make(Duration.fromSeconds(sendTimeoutSeconds))
      retry <- Retry.make[Any, DomainError](
        Retry.Schedules.whenCase[Any, DomainError, (Any, Long)]({ case TelegramSendFailed(_) => () }) {
          Retry.Schedules.common(
            min = Duration.fromMillis(10),
            max = Duration.fromMillis(50),
            factor = 2.0,
            maxRetries = Some(retryMaxRetries)
          )
        }
      )
    yield TelegramChannel(raw, rateLimiter, circuitBreaker, timeout, retry)

  def spec: Spec[Any, Any] =
    suite("TelegramChannel")(
      test("успешная отправка (sanity)") {
        Live.live {
          ZIO.scoped {
            for
              channel <- buildChannel(succeedRaw)
              result <- channel.send(message).either
            yield assert(result)(isRight(anything))
          }
        }
      },
      suite("M-NTF-08 retry")(
        test("недоступный Telegram -> 5 попыток (1 + 4 retry), затем TelegramSendFailed") {
          Live.live {
            ZIO.scoped {
              for
                calls <- Ref.make(0)
                channel <- buildChannel(failingRaw(calls), retryMaxRetries = 4)
                result <- channel.send(message).either
                total <- calls.get
              yield assert(result)(isLeft(isSubtype[TelegramSendFailed](anything))) && assertTrue(total == 5)
            }
          }
        },
        test("retry отсутствует (maxRetries = 0) -> ровно 1 попытка") {
          Live.live {
            ZIO.scoped {
              for
                calls <- Ref.make(0)
                channel <- buildChannel(failingRaw(calls), retryMaxRetries = 0)
                result <- channel.send(message).either
                total <- calls.get
              yield assert(result)(isLeft(isSubtype[TelegramSendFailed](anything))) && assertTrue(total == 1)
            }
          }
        }
      ),
      suite("M-NTF-09 circuit breaker")(
        test("5 ошибок подряд -> CB открыт, 6-я отправка fast-fail без вызова клиента") {
          Live.live {
            ZIO.scoped {
              for
                calls <- Ref.make(0)
                channel <- buildChannel(failingRaw(calls), cbMaxFailures = 5, retryMaxRetries = 0)
                firstFive <- ZIO.foreach(1 to 5)(_ => channel.send(message).either)
                afterFive <- calls.get
                sixth <- channel.send(message).either
                afterSixth <- calls.get
              yield assertTrue(firstFive.forall(_.isLeft)) &&
                assertTrue(afterFive == 5) &&
                assert(sixth)(
                  isLeft(
                    isSubtype[TelegramSendFailed](hasField("message", _.message, containsString("circuit breaker")))
                  )
                ) &&
                assertTrue(afterSixth == 5)
            }
          }
        }
      ),
      suite("M-NTF-10 rate limiter")(
        test("при лимите 2 msg/s параллельные отправки не превышают 2 одновременно") {
          Live.live {
            ZIO.scoped {
              for
                inFlight <- Ref.make(0)
                maxConcurrent <- Ref.make(0)
                channel <- buildChannel(concurrentRaw(50, inFlight, maxConcurrent), ratePerSecond = 2)
                results <- ZIO.foreachPar(1 to 5)(_ => channel.send(message).either)
                peak <- maxConcurrent.get
              yield assertTrue(results.forall(_.isRight)) && assertTrue(peak <= 2)
            }
          }
        }
      ),
      suite("M-NTF-11 time limiter")(
        test("запрос дольше таймаута -> TelegramSendFailed «timed out» (не дожидаясь ответа)") {
          Live.live {
            ZIO.scoped {
              for
                channel <- buildChannel(slowRaw(5000), sendTimeoutSeconds = 1)
                result <- channel.send(message).either.timeout(3.seconds)
              yield assert(result)(
                isSome(
                  isLeft(isSubtype[TelegramSendFailed](hasField("message", _.message, containsString("timed out"))))
                )
              )
            }
          }
        }
      )
    ) @@ TestAspect.timeout(15.seconds)
