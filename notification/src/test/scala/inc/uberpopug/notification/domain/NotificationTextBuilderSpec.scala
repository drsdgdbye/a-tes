package inc.uberpopug.notification.domain

import java.time.{Instant, LocalDate}

import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.common.domain.UserId

/** Тесты SSOT форматов сообщений (M-NTF-12): типы событий, форматирование денег и дат. */
object NotificationTextBuilderSpec extends ZIOSpecDefault:

  private val popug = UserId(java.util.UUID.randomUUID())
  private val ts = Instant.ofEpochMilli(1000L)

  private val assignedEvent = NotificationEvent.TaskAssigned(popug, "Fix bug", ts)
  private val completedEvent = NotificationEvent.TaskCompleted(popug, "Fix bug", 1234L, ts)
  private val paymentEvent = NotificationEvent.PaymentProcessed(popug, 259999L, LocalDate.of(2026, 1, 15), ts)

  def spec: Spec[Any, Any] =
    suite("NotificationTextBuilder")(
      suite("M-NTF-12 render formats")(
        test("TaskAssigned -> «Вам назначена задача «title»»") {
          assertTrue(
            NotificationTextBuilder.render(assignedEvent) == "Вам назначена задача «Fix bug»"
          )
        },
        test("TaskCompleted -> «Задача «title» выполнена. Начислено: $12.34»") {
          assertTrue(
            NotificationTextBuilder.render(completedEvent) == "Задача «Fix bug» выполнена. Начислено: $12.34"
          )
        },
        test("PaymentProcessed -> «Выплата за DD.MM: $XX» (формат из спеки)") {
          assertTrue(
            NotificationTextBuilder.render(paymentEvent) == "Выплата за 15.01: $2599.99"
          )
        }
      ),
      suite("money formatting")(
        test("zero cents -> $0.00") {
          assertTrue(NotificationTextBuilder.completed("t", 0L) == "Задача «t» выполнена. Начислено: $0.00")
        },
        test("5 cents -> $0.05") {
          assertTrue(NotificationTextBuilder.completed("t", 5L) == "Задача «t» выполнена. Начислено: $0.05")
        },
        test("100 cents -> $1.00") {
          assertTrue(NotificationTextBuilder.completed("t", 100L) == "Задача «t» выполнена. Начислено: $1.00")
        },
        test("999 cents -> $9.99") {
          assertTrue(NotificationTextBuilder.completed("t", 999L) == "Задача «t» выполнена. Начислено: $9.99")
        },
        test("259999 cents -> $2599.99") {
          assertTrue(NotificationTextBuilder.completed("t", 259999L) == "Задача «t» выполнена. Начислено: $2599.99")
        },
        test("negative cents -> $-x.yy (не появляются в реальных событиях, но не падает)") {
          assertTrue(NotificationTextBuilder.completed("t", -1234L) == "Задача «t» выполнена. Начислено: $-12.34")
        }
      ),
      suite("date formatting")(
        test("15 January -> 15.01") {
          assertTrue(NotificationTextBuilder.payment(100L, LocalDate.of(2026, 1, 15)) == "Выплата за 15.01: $1.00")
        },
        test("5 March -> 05.03 (zero-padded)") {
          assertTrue(NotificationTextBuilder.payment(100L, LocalDate.of(2026, 3, 5)) == "Выплата за 05.03: $1.00")
        },
        test("30 November -> 30.11") {
          assertTrue(NotificationTextBuilder.payment(100L, LocalDate.of(2026, 11, 30)) == "Выплата за 30.11: $1.00")
        }
      ),
      suite("property-based")(
        test("любые центы форматируются как $<digits>.<2dp>") {
          check(Gen.long) { cents =>
            val rendered = NotificationTextBuilder.completed("t", cents)
            assert(rendered)(matchesRegex("Задача «t» выполнена. Начислено: \\$-?\\d+\\.\\d{2}"))
          }
        },
        test("любая дата форматируется как DD.MM с нулевым паддингом") {
          check(Gen.int(1, 28), Gen.int(1, 12)) { (day, month) =>
            val date = LocalDate.of(2026, month, day)
            val rendered = NotificationTextBuilder.payment(100L, date)
            val expected = f"$day%02d.$month%02d"
            assert(rendered)(startsWithString(s"Выплата за $expected:"))
          }
        }
      )
    )
