package inc.uberpopug.accounting.service

import java.time.LocalDate
import java.util.UUID

import zio.{Clock, Ref, ZIO}
import zio.test.*

import inc.uberpopug.accounting.{InMemoryEventStore, InMemoryEventStoreState, orDieE}
import inc.uberpopug.accounting.domain.AccountEvent
import inc.uberpopug.common.domain.UserId

import auth.user_created.UserCreated
import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted

/** Тесты ежедневных выплат: сумма, обнуление, долг, срез по времени, outbox и идемпотентность (M-ACC-14..19). */
object PayoutServiceSpec extends ZIOSpecDefault:
  private val payoutDate = LocalDate.of(2026, 1, 15)
  private val payoutAt = payoutDate.atTime(23, 0).toInstant(java.time.ZoneOffset.UTC)

  private def userId(): UserId = UserId(UUID.randomUUID())

  private def makeService: ZIO[
    Any,
    Nothing,
    (PayoutServiceLive, InMemoryEventStore, Ref[InMemoryEventStoreState], Clock)
  ] =
    for
      clock <- ZIO.clock
      state <- Ref.make(InMemoryEventStoreState())
      store = InMemoryEventStore(state)
      service = PayoutServiceLive(store, clock)
    yield (service, store, state, clock)

  /** Регистрирует пользователя и при желании выполняет assign/complete до указанного времени. */
  private def seed(
      store: InMemoryEventStore,
      popug: UUID,
      fee: Option[Long] = None,
      reward: Option[Long] = None,
      ts: Long = 1000L
  ): ZIO[Any, Nothing, Unit] =
    val userEvent =
      UserCreated(UUID.randomUUID().toString, ts - 500, 1, popug.toString, "Popug", "p@popug.inc", "popug")
    val register = AccountingEventProcessor.processUserCreated(userEvent, store).orDieE
    val withAssign = fee.fold(register)(feeValue =>
      register *> AccountingEventProcessor
        .processTaskAssigned(
          TaskAssigned(
            UUID.randomUUID().toString,
            ts,
            1,
            UUID.randomUUID().toString,
            "Task",
            popug.toString,
            "",
            feeValue
          ),
          store
        )
        .orDieE
    )
    val withReward = reward.fold(withAssign)(rewardValue =>
      withAssign *> AccountingEventProcessor
        .processTaskCompleted(
          TaskCompleted(
            UUID.randomUUID().toString,
            ts + 100,
            1,
            UUID.randomUUID().toString,
            "Task",
            popug.toString,
            rewardValue
          ),
          store
        )
        .orDieE
    )
    withReward

  def spec: Spec[Any, Any] =
    suite("PayoutService")(
      test("M-ACC-14 positive balance -> payout = balance, account zeroed") {
        for
          (service, store, state, _) <- makeService
          popug = userId()
          _ <- seed(store, popug.value, fee = Some(1500L), reward = Some(2500L))
          _ <- TestClock.setTime(payoutAt)
          _ <- service.runOnce.orDieE
          balance <- store.balanceOf(popug).orDieE
          payoutsList <- state.get.map(_.events.collect { case e: AccountEvent.AccountPayout => e })
          outbox <- state.get.map(_.outbox.toList)
        yield assertTrue(balance.exists(_.toCents == 0L)) &&
          assertTrue(payoutsList.size == 1) &&
          assertTrue(payoutsList.head.amount.toCents == 1000L) &&
          assertTrue(payoutsList.head.deltaCents == -1000L) &&
          assertTrue(outbox.size == 1) &&
          assertTrue(outbox.head.eventType == "PaymentProcessed")
      },
      test("M-ACC-15 zero balance -> payout 0, nothing changes") {
        for
          (service, store, state, _) <- makeService
          popug = userId()
          _ <- seed(store, popug.value)
          _ <- TestClock.setTime(payoutAt)
          _ <- service.runOnce.orDieE
          balance <- store.balanceOf(popug).orDieE
          payouts <- state.get.map(_.events.collect { case e: AccountEvent.AccountPayout => e })
          outbox <- state.get.map(_.outbox.toList)
        yield assertTrue(balance.exists(_.toCents == 0L)) &&
          assertTrue(payouts.head.amount.toCents == 0L) &&
          assertTrue(outbox.size == 1)
      },
      test("M-ACC-16 negative balance -> payout 0, debt preserved") {
        for
          (service, store, state, _) <- makeService
          popug = userId()
          _ <- seed(store, popug.value, fee = Some(3000L))
          _ <- TestClock.setTime(payoutAt)
          _ <- service.runOnce.orDieE
          balance <- store.balanceOf(popug).orDieE
          payouts <- state.get.map(_.events.collect { case e: AccountEvent.AccountPayout => e })
        yield assertTrue(balance.exists(_.toCents == -3000L)) &&
          assertTrue(payouts.head.amount.toCents == 0L) &&
          assertTrue(payouts.head.deltaCents == 0L)
      },
      test("M-ACC-17 cutoff: events after payout run go to the next day") {
        for
          (service, store, state, _) <- makeService
          popug = userId()
          _ <- seed(store, popug.value, reward = Some(2500L))
          _ <- TestClock.setTime(payoutAt)
          _ <- service.runOnce.orDieE
          balanceAfterFirst <- store.balanceOf(popug).orDieE
          _ <- AccountingEventProcessor
            .processTaskCompleted(
              TaskCompleted(
                UUID.randomUUID().toString,
                2330L,
                1,
                UUID.randomUUID().toString,
                "Task",
                popug.value.toString,
                1500L
              ),
              store
            )
            .orDieE
          nextDay = payoutDate.plusDays(1).atTime(23, 0).toInstant(java.time.ZoneOffset.UTC)
          _ <- TestClock.setTime(nextDay)
          _ <- service.runOnce.orDieE
          balanceAfterSecond <- store.balanceOf(popug).orDieE
          payouts <- state.get.map(_.events.collect { case e: AccountEvent.AccountPayout => e })
        yield assertTrue(balanceAfterFirst.exists(_.toCents == 0L)) &&
          assertTrue(balanceAfterSecond.exists(_.toCents == 0L)) &&
          assertTrue(payouts.size == 2) &&
          assertTrue(payouts(1).amount.toCents == 1500L)
      },
      test("M-ACC-18 outbox holds one PaymentProcessed per account") {
        for
          (service, store, state, _) <- makeService
          popug1 = userId()
          popug2 = userId()
          popug3 = userId()
          _ <- seed(store, popug1.value, fee = Some(1500L), reward = Some(2500L))
          _ <- seed(store, popug2.value)
          _ <- seed(store, popug3.value, fee = Some(3000L))
          _ <- TestClock.setTime(payoutAt)
          _ <- service.runOnce.orDieE
          outbox <- state.get.map(_.outbox.toList)
          processed = outbox.map(r => (r.aggregateId, r.eventType))
          payouts <- state.get.map(_.events.collect { case e: AccountEvent.AccountPayout => e })
        yield assertTrue(outbox.size == 3) &&
          assertTrue(processed.forall(_._2 == "PaymentProcessed")) &&
          assertTrue(processed.map(_._1).toSet == Set(popug1.value, popug2.value, popug3.value)) &&
          assertTrue(payouts.size == 3)
      },
      test("M-ACC-19 repeated cron run is idempotent (deterministic event_id)") {
        for
          (service, store, state, _) <- makeService
          popug = userId()
          _ <- seed(store, popug.value, fee = Some(1500L), reward = Some(2500L))
          _ <- TestClock.setTime(payoutAt)
          _ <- service.runOnce.orDieE
          outboxAfterFirst <- state.get.map(_.outbox.size)
          _ <- service.runOnce.orDieE
          outboxAfterSecond <- state.get.map(_.outbox.size)
          payouts <- state.get.map(_.events.collect { case e: AccountEvent.AccountPayout => e })
        yield assertTrue(outboxAfterFirst == 1) &&
          assertTrue(outboxAfterSecond == 1) &&
          assertTrue(payouts.size == 1)
      }
    )
