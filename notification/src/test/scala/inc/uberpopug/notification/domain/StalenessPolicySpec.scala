package inc.uberpopug.notification.domain

import java.time.{Duration, Instant}

import zio.test.*

/** Тесты защиты от лавины уведомлений (M-NTF-05/06): событие старше 5 минут не отправляется никому. */
object StalenessPolicySpec extends ZIOSpecDefault:

  private val now = Instant.parse("2026-01-15T10:00:00Z")

  def spec: Spec[Any, Any] =
    suite("StalenessPolicy")(
      suite("boundary 5 minutes")(
        test("событие ровно 5 минут назад -> НЕ stale (граница включена в допустимый возраст)") {
          assertTrue(!StalenessPolicy.isStale(now.minus(StalenessPolicy.maxEventAge), now))
        },
        test("событие 4 минуты 59 секунд назад -> НЕ stale") {
          assertTrue(!StalenessPolicy.isStale(now.minusSeconds(4 * 60 + 59), now))
        },
        test("событие 5 минут 1 секунду назад -> stale") {
          assertTrue(StalenessPolicy.isStale(now.minusSeconds(5 * 60 + 1), now))
        },
        test("событие 10 минут назад -> stale") {
          assertTrue(StalenessPolicy.isStale(now.minus(Duration.ofMinutes(10)), now))
        },
        test("будущее событие (часы ушли вперёд) -> НЕ stale") {
          assertTrue(!StalenessPolicy.isStale(now.plusSeconds(60), now))
        }
      ),
      suite("property-based boundary")(
        test("isStale эквивалентно age > 5 минут для любого возраста") {
          check(Gen.long(0L, 10L * 60 * 60 * 1000)) { ageMillis =>
            val age = Duration.ofMillis(ageMillis)
            val stale = StalenessPolicy.isStale(now.minus(age), now)
            val expected = age.compareTo(StalenessPolicy.maxEventAge) > 0
            assertTrue(stale == expected)
          }
        }
      )
    )
