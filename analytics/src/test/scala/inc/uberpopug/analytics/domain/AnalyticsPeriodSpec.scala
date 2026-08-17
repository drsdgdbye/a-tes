package inc.uberpopug.analytics.domain

import java.time.LocalDate

import zio.test.*
import zio.test.Assertion.*

/** Property-based проверка окон агрегации (см. ADR/M-ANL-10..11): окно включительно, день = `[D, D]`, неделя = `[D,
  * D+6]`, месяц = `[D, D+29]`.
  */
object AnalyticsPeriodSpec extends ZIOSpecDefault:
  private val anyDate = Gen.localDate(
    LocalDate.of(2020, 1, 1),
    LocalDate.of(2030, 12, 31)
  )

  def spec: Spec[Any, Any] =
    suite("AnalyticsPeriod.windowFrom")(
      test("day window is exactly [D, D]") {
        check(anyDate) { d =>
          val (from, to) = AnalyticsPeriod.Day.windowFrom(d)
          assertTrue(from == d, to == d)
        }
      },
      test("week window is [D, D+6]") {
        check(anyDate) { d =>
          val (from, to) = AnalyticsPeriod.Week.windowFrom(d)
          assertTrue(from == d, to == d.plusDays(6))
        }
      },
      test("month window is [D, D+29]") {
        check(anyDate) { d =>
          val (from, to) = AnalyticsPeriod.Month.windowFrom(d)
          assertTrue(from == d, to == d.plusDays(29))
        }
      },
      test("window is inclusive (from <= to and bounds inside window)") {
        check(anyDate, Gen.elements(AnalyticsPeriod.Day, AnalyticsPeriod.Week, AnalyticsPeriod.Month)) { (d, period) =>
          val (from, to) = period.windowFrom(d)
          assertTrue(!from.isAfter(to), from.isEqual(d), !to.isBefore(d))
        }
      },
      test("from parses and round-trips wire values") {
        check(Gen.elements("day", "week", "month")) { wire =>
          assert(AnalyticsPeriod.from(wire))(isRight(hasField("wire", _.wire, equalTo(wire))))
        }
      },
      test("unknown period string -> InvalidValue") {
        assert(AnalyticsPeriod.from("year"))(isLeft(anything))
      }
    )
