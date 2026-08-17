package inc.uberpopug.accounting.service

import java.time.{Instant, LocalDate}
import java.util.UUID

import zio.{Clock, Ref, ZIO, ZLayer}
import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.accounting.{InMemoryEventStore, InMemoryEventStoreState, orDieE}
import inc.uberpopug.accounting.domain.{AccountEvent, AuditLogEntry, Role}
import inc.uberpopug.common.domain.UserId
import inc.uberpopug.common.domain.DomainError.{AccessDenied, AccountNotFound}

import auth.user_created.UserCreated
import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted
import task.task_created.TaskCreated

/** Тесты AccountingService: баланс, аудитлог, доход менеджмента, ежедневная статистика и права доступа (M-ACC-08..13,
  * 20..24).
  */
object AccountingServiceSpec extends ZIOSpecDefault:
  private val day = LocalDate.of(2026, 1, 15)

  private def at(hour: Int, minute: Int): Instant =
    day.atTime(hour, minute).toInstant(java.time.ZoneOffset.UTC)

  private def userId(): UserId = UserId(UUID.randomUUID())

  private def actor(role: Role, id: UserId = userId()): AuthenticatedUser = AuthenticatedUser(id, role)

  /** Понижает env-требование эффекта с `Clock` до `Any` (live clock для тестов). */
  private def withLiveClock[E, A](effect: ZIO[Clock, E, A]): ZIO[Any, E, A] =
    effect.provideLayer(ZLayer.succeed(Clock.ClockLive))

  private def makeService: ZIO[
    Any,
    Nothing,
    (AccountingServiceLive, InMemoryEventStore, Ref[InMemoryEventStoreState])
  ] =
    for
      state <- Ref.make(InMemoryEventStoreState())
      store = InMemoryEventStore(state)
      service = AccountingServiceLive(store)
    yield (service, store, state)

  /** Регистрирует пользователя и выполняет полный жизненный цикл задачи (create + assign + complete). */
  private def seedLifecycle(
      processor: AccountingEventProcessor.type,
      store: InMemoryEventStore,
      popug: UUID,
      fee: Long = 1500L,
      reward: Long = 2500L,
      tsCreated: Long = 500L,
      tsAssign: Long = 1000L,
      tsComplete: Long = 2000L
  ): ZIO[Any, Nothing, Unit] =
    val userEvent =
      UserCreated(UUID.randomUUID().toString, tsCreated - 500, 1, popug.toString, "Popug", "p@popug.inc", "popug")
    val taskId = UUID.randomUUID()
    val created =
      TaskCreated(UUID.randomUUID().toString, tsCreated, 1, taskId.toString, "Task", "", popug.toString, fee, reward)
    val assign = TaskAssigned(UUID.randomUUID().toString, tsAssign, 1, taskId.toString, "Task", popug.toString, "", fee)
    val complete =
      TaskCompleted(UUID.randomUUID().toString, tsComplete, 1, taskId.toString, "Task", popug.toString, reward)
    for
      _ <- processor.processUserCreated(userEvent, store).orDieE
      _ <- processor.processTaskCreated(created, store).orDieE
      _ <- processor.processTaskAssigned(assign, store).orDieE
      _ <- processor.processTaskCompleted(complete, store).orDieE
    yield ()

  def spec: Spec[Any, Any] =
    suite("AccountingService")(
      suite("M-ACC-08..10 balance")(
        test("M-ACC-08 balance = sum(credited) - sum(debited)") {
          for
            (service, store, _) <- makeService
            popug = userId()
            _ <- seedLifecycle(AccountingEventProcessor, store, popug.value)
            snapshot <- withLiveClock(service.balanceOf(actor(Role.Popug, popug)))
          yield assertTrue(snapshot.balance.toCents == 1000L)
        },
        test("M-ACC-09 negative balance is allowed") {
          for
            (service, store, _) <- makeService
            popug = userId()
            _ <- seedLifecycle(AccountingEventProcessor, store, popug.value, fee = 3000L, reward = 1000L)
            snapshot <- withLiveClock(service.balanceOf(actor(Role.Popug, popug)))
          yield assertTrue(snapshot.balance.toCents == -2000L)
        },
        test("M-ACC-10 balance for unknown account -> AccountNotFound") {
          for
            (service, _, _) <- makeService
            result <- withLiveClock(service.balanceOf(actor(Role.Popug)).either)
          yield assert(result)(isLeft(isSubtype[AccountNotFound](anything)))
        }
      ),
      suite("M-ACC-11..13 daily aggregation")(
        test("M-ACC-11 dailyBalance only counts events of the given date") {
          for
            (_, store, _) <- makeService
            popug = userId()
            _ <- seedLifecycle(
              AccountingEventProcessor,
              store,
              popug.value,
              tsCreated = at(8, 0).toEpochMilli,
              tsAssign = at(9, 0).toEpochMilli,
              tsComplete = at(18, 0).toEpochMilli
            )
            events <- store.eventsBetween(at(0, 0), day.plusDays(1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant)
            daily = inc.uberpopug.accounting.domain.BalanceCalculator.dailyBalance(events, day)
          yield assertTrue(daily.toCents == 1000L)
        },
        test("M-ACC-12 managementEarnings = sum(assignFee) - sum(completeReward), can be negative") {
          for
            (service, store, _) <- makeService
            popug1 = userId()
            popug2 = userId()
            _ <- seedLifecycle(
              AccountingEventProcessor,
              store,
              popug1.value,
              tsCreated = at(8, 0).toEpochMilli,
              tsAssign = at(9, 0).toEpochMilli,
              tsComplete = at(10, 0).toEpochMilli
            )
            _ <- seedLifecycle(
              AccountingEventProcessor,
              store,
              popug2.value,
              reward = 2000L,
              tsCreated = at(11, 0).toEpochMilli,
              tsAssign = at(12, 0).toEpochMilli,
              tsComplete = at(13, 0).toEpochMilli
            )
            earnings <- service.managementEarnings(day, actor(Role.Admin))
          yield assertTrue(earnings.toCents == -1500L)
        },
        test("M-ACC-13 aggregation over 10+ events per day") {
          for
            (service, store, _) <- makeService
            popugs = List.fill(10)(userId())
            _ <- ZIO.foreachDiscard(popugs.zipWithIndex) { case (popug, i) =>
              seedLifecycle(
                AccountingEventProcessor,
                store,
                popug.value,
                fee = 1000L,
                reward = 3000L,
                tsCreated = at(8, 0).toEpochMilli + i * 1000L,
                tsAssign = at(9, 0).toEpochMilli + i * 1000L,
                tsComplete = at(18, 0).toEpochMilli + i * 1000L
              )
            }
            daily <- service.dailyStats(day, day, actor(Role.Accountant))
          yield assertTrue(daily.size == 1) &&
            assertTrue(daily.head.earnings.toCents == 10 * (1000L - 3000L)) &&
            assertTrue(daily.head.popugsTotal == 10) &&
            assertTrue(daily.head.popugsNegative == 0)
        }
      ),
      suite("M-ACC-20..24 audit log")(
        test("M-ACC-20/21 assign, complete and refund entries carry a type and taskId") {
          for
            (service, store, _) <- makeService
            old = userId()
            current = userId()
            taskId = UUID.randomUUID()
            userOld = UserCreated(
              UUID.randomUUID().toString,
              100L,
              1,
              old.value.toString,
              "Old",
              "o@popug.inc",
              "popug"
            )
            userCur = UserCreated(
              UUID.randomUUID().toString,
              100L,
              1,
              current.value.toString,
              "Cur",
              "c@popug.inc",
              "popug"
            )
            _ <- AccountingEventProcessor.processUserCreated(userOld, store).orDieE
            _ <- AccountingEventProcessor.processUserCreated(userCur, store).orDieE
            _ <- AccountingEventProcessor
              .processTaskAssigned(
                TaskAssigned(
                  UUID.randomUUID().toString,
                  200L,
                  1,
                  taskId.toString,
                  "T",
                  current.value.toString,
                  old.value.toString,
                  1500L
                ),
                store
              )
              .orDieE
            _ <- AccountingEventProcessor
              .processTaskCompleted(
                TaskCompleted(UUID.randomUUID().toString, 300L, 1, taskId.toString, "T", current.value.toString, 2500L),
                store
              )
              .orDieE
            (entriesCur, _) <- service.auditLog(actor(Role.Popug, current), 10, 0)
            (entriesOld, _) <- service.auditLog(actor(Role.Popug, old), 10, 0)
            entries = entriesCur ++ entriesOld
            types = entries.map(_.`type`).toSet
          yield assertTrue(entries.size == 3) &&
            assertTrue(types == Set(AuditLogEntry.TypeAssign, AuditLogEntry.TypeRefund, AuditLogEntry.TypeComplete)) &&
            assertTrue(entries.forall(_.taskId.isDefined)) &&
            assertTrue(entries.forall(_.`type`.nonEmpty))
        },
        test("M-ACC-22 payout entry has taskId = None") {
          for
            (service, store, state) <- makeService
            popug = userId()
            _ <- seedLifecycle(AccountingEventProcessor, store, popug.value)
            payoutId = inc.uberpopug.accounting.domain.PayoutCalculator.payoutEventId(popug, day)
            _ <- state.update(s =>
              s.copy(events =
                s.events :+ AccountEvent.AccountPayout(
                  payoutId,
                  at(23, 0),
                  popug,
                  inc.uberpopug.common.domain.Money.fromCents(1000L),
                  day
                )
              )
            )
            (entries, _) <- service.auditLog(actor(Role.Popug, popug), 10, 0)
          yield assertTrue(entries.exists(e => e.`type` == AuditLogEntry.TypePayout && e.taskId.isEmpty))
        },
        test("M-ACC-23 entries are sorted by timestamp desc (newest first)") {
          for
            (service, store, _) <- makeService
            popug = userId()
            _ <- seedLifecycle(AccountingEventProcessor, store, popug.value)
            (entries, _) <- service.auditLog(actor(Role.Popug, popug), 10, 0)
          yield assertTrue(entries == entries.sortBy(_.timestamp)(using Ordering[Instant].reverse))
        },
        test("M-ACC-24 pagination respects limit/offset") {
          for
            (service, store, _) <- makeService
            popug = userId()
            _ <- seedLifecycle(AccountingEventProcessor, store, popug.value)
            (page1, total) <- service.auditLog(actor(Role.Popug, popug), 2, 0)
            (page2, _) <- service.auditLog(actor(Role.Popug, popug), 2, 2)
            (page3, _) <- service.auditLog(actor(Role.Popug, popug), 2, 4)
          yield assertTrue(total == 2L) &&
            assertTrue(page1.size == 2) &&
            assertTrue(page2.isEmpty) &&
            assertTrue(page3.isEmpty)
        }
      ),
      suite("access control")(
        test("managementEarnings requires admin or accountant") {
          for
            (service, _, _) <- makeService
            forbidden <- service.managementEarnings(day, actor(Role.Popug)).either
          yield assert(forbidden)(isLeft(isSubtype[AccessDenied](anything)))
        },
        test("dailyStats requires admin or accountant") {
          for
            (service, _, _) <- makeService
            forbidden <- service.dailyStats(day, day, actor(Role.Popug)).either
          yield assert(forbidden)(isLeft(isSubtype[AccessDenied](anything)))
        },
        test("balance and audit log are allowed for any role") {
          for
            (service, store, _) <- makeService
            popug = userId()
            _ <- seedLifecycle(AccountingEventProcessor, store, popug.value)
            balance <- withLiveClock(service.balanceOf(actor(Role.Popug, popug)).orDieE)
            (entries, _) <- service.auditLog(actor(Role.Popug, popug), 10, 0).orDieE
          yield assertTrue(balance.balance.toCents == 1000L) && assertTrue(entries.size == 2)
        }
      )
    )
