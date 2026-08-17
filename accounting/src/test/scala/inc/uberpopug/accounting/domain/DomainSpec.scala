package inc.uberpopug.accounting.domain

import java.time.{Instant, LocalDate}
import java.util.UUID

import zio.test.*

import inc.uberpopug.common.domain.{Money, TaskId, UserId}

/** Юнит-тесты чистых доменных политик: баланс, выплаты, доход менеджмента и ежедневная статистика. Включает
  * property-based тесты с генераторами данных.
  */
object DomainSpec extends ZIOSpecDefault:
  private val day = LocalDate.of(2026, 3, 15)

  private def mkCredited(
      uid: UUID,
      cents: Long,
      ts: Instant,
      reason: AccountEvent.CreditReason = AccountEvent.CreditReason.TaskCompleted
  ): AccountEvent =
    AccountEvent.AccountCredited(
      eventId = UUID.randomUUID(),
      timestamp = ts,
      userId = UserId(uid),
      amount = Money.fromCents(cents),
      taskId = TaskId(UUID.randomUUID()),
      reason = reason
    )

  private def mkDebited(uid: UUID, cents: Long, ts: Instant): AccountEvent =
    AccountEvent.AccountDebited(
      eventId = UUID.randomUUID(),
      timestamp = ts,
      userId = UserId(uid),
      amount = Money.fromCents(cents),
      taskId = TaskId(UUID.randomUUID()),
      reason = AccountEvent.DebitReason.TaskAssigned
    )

  /** Генератор случайных финансовых событий (кредиты и дебеты) с центом. */
  private val genEvent: Gen[Any, AccountEvent] =
    Gen.oneOf(
      Gen.long(1L, 100000L).map(c => mkCredited(UUID.randomUUID(), c, Instant.parse("2026-03-15T12:00:00Z"))),
      Gen.long(1L, 100000L).map(c => mkDebited(UUID.randomUUID(), c, Instant.parse("2026-03-15T12:00:00Z")))
    )

  def spec: Spec[Any, Any] =
    suite("Domain")(
      suite("BalanceCalculator")(
        test("M-ACC-08 currentBalance equals sum of deltas (property)") {
          check(Gen.listOf(genEvent)) { events =>
            val expected = events.map(_.deltaCents).sum
            assertTrue(BalanceCalculator.currentBalance(events).toCents == expected)
          }
        },
        test("dailyBalance only includes events on the given date") {
          val inDay = mkCredited(UUID.randomUUID(), 500L, Instant.parse("2026-03-15T12:00:00Z"))
          val nextDay = mkCredited(UUID.randomUUID(), 700L, Instant.parse("2026-03-16T00:00:01Z"))
          val events = List(inDay, nextDay)
          assertTrue(BalanceCalculator.dailyBalance(events, day).toCents == 500L)
        }
      ),
      suite("PayoutCalculator")(
        test("M-ACC-14/16 payoutAmount = max(0, balance) (property)") {
          check(Gen.long(-500000L, 500000L)) { cents =>
            val balance = Money.fromCents(cents)
            val amount = PayoutCalculator.payoutAmount(balance)
            val expected = if cents > 0 then cents else 0L
            assertTrue(amount.toCents == expected)
          }
        },
        test("payoutEventId is deterministic per user/date and differs across dates") {
          val uid = UserId(UUID.randomUUID())
          val d1 = day
          val d2 = day.plusDays(1)
          assertTrue(PayoutCalculator.payoutEventId(uid, d1) == PayoutCalculator.payoutEventId(uid, d1)) &&
          assertTrue(PayoutCalculator.payoutEventId(uid, d1) != PayoutCalculator.payoutEventId(uid, d2)) &&
          assertTrue(
            PayoutCalculator.payoutEventId(uid, d1) != PayoutCalculator.payoutEventId(UserId(UUID.randomUUID()), d1)
          )
        }
      ),
      suite("ManagementEarnings")(
        test("M-ACC-12 earnings = sum(assignFee) - sum(completeReward), can be negative") {
          val recorded = List(
            AccountEvent.TaskPriceRecorded(
              UUID.randomUUID(),
              Instant.parse("2026-03-15T10:00:00Z"),
              taskId = TaskId(UUID.randomUUID()),
              userId = UserId(UUID.randomUUID()),
              assignFee = Money.fromCents(1000L),
              completeReward = Money.fromCents(2500L)
            ),
            AccountEvent.TaskPriceRecorded(
              UUID.randomUUID(),
              Instant.parse("2026-03-15T11:00:00Z"),
              taskId = TaskId(UUID.randomUUID()),
              userId = UserId(UUID.randomUUID()),
              assignFee = Money.fromCents(2000L),
              completeReward = Money.fromCents(1000L)
            )
          )
          val nextDay = AccountEvent.TaskPriceRecorded(
            UUID.randomUUID(),
            Instant.parse("2026-03-16T00:00:01Z"),
            taskId = TaskId(UUID.randomUUID()),
            userId = UserId(UUID.randomUUID()),
            assignFee = Money.fromCents(5000L),
            completeReward = Money.fromCents(0L)
          )
          val earnings = ManagementEarnings.forDate(recorded :+ nextDay, day)
          assertTrue(earnings.toCents == (1000L - 2500L) + (2000L - 1000L))
        }
      ),
      suite("DailyStatsCalculator")(
        test("daily stats aggregate per-day earnings and popug counts") {
          val from = LocalDate.of(2026, 3, 15)
          val to = LocalDate.of(2026, 3, 17)
          val userA = UUID.randomUUID()
          val userB = UUID.randomUUID()
          val userC = UUID.randomUUID()
          val users = List(
            DailyStatsCalculator.UserSnapshot(userA, "A", Instant.parse("2026-03-15T00:00:00Z")),
            DailyStatsCalculator.UserSnapshot(userB, "B", Instant.parse("2026-03-15T00:00:00Z")),
            DailyStatsCalculator.UserSnapshot(userC, "C", Instant.parse("2026-03-17T00:00:00Z"))
          )
          val events = List(
            mkDebited(userA, 2000L, Instant.parse("2026-03-15T12:00:00Z")),
            mkDebited(userB, 1000L, Instant.parse("2026-03-15T13:00:00Z")),
            mkCredited(userA, 5000L, Instant.parse("2026-03-16T12:00:00Z")),
            AccountEvent.TaskPriceRecorded(
              UUID.randomUUID(),
              Instant.parse("2026-03-16T10:00:00Z"),
              taskId = TaskId(UUID.randomUUID()),
              userId = UserId(userA),
              assignFee = Money.fromCents(3000L),
              completeReward = Money.fromCents(4000L)
            )
          )
          val stats = DailyStatsCalculator.build(from, to, events, users)
          assertTrue(stats.size == 3) &&
          assertTrue(
            stats.head.date == from && stats.head.earnings.toCents == 0L && stats.head.popugsTotal == 2 && stats.head.popugsNegative == 2
          ) &&
          assertTrue(stats(1).earnings.toCents == -1000L && stats(1).popugsTotal == 2) &&
          assertTrue(stats(2).popugsTotal == 3)
        },
        test("popugsNegative counts accounts with negative balance at day end (property)") {
          check(Gen.listOfBounded(0, 30)(genEvent)) { events =>
            val day = LocalDate.of(2026, 3, 15)
            val users =
              List(DailyStatsCalculator.UserSnapshot(UUID.randomUUID(), "A", Instant.parse("2026-03-15T00:00:00Z")))
            val stats = DailyStatsCalculator.build(day, day, events, users)
            val endOfDay = BalanceCalculator.endOfDay(day)
            val included = events.filter(_.timestamp.isBefore(endOfDay))
            val byUser = included.groupBy(_.aggregateId).view.mapValues(BalanceCalculator.currentBalance).toMap
            val expectedNegative = byUser.values.count(_.isNegative)
            assertTrue(stats.size == 1 && stats.head.popugsNegative == expectedNegative)
          }
        }
      )
    )
