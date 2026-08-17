package inc.uberpopug.analytics.service

import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID

import zio.{Ref, ZIO}
import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.analytics.{InMemoryAnalyticsState, InMemoryAnalyticsStore, orDieE}
import inc.uberpopug.analytics.domain.{AnalyticsPeriod, Role}
import inc.uberpopug.common.domain.DomainError.AccessDenied
import inc.uberpopug.common.domain.UserId

import auth.user_created.UserCreated
import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted
import task.task_created.TaskCreated

/** Тесты AnalyticsService: доход менеджмента, попуги в минусе, самая дорогая задача и права доступа (M-ANL-06..14). */
object AnalyticsServiceSpec extends ZIOSpecDefault:
  private def makeService
      : ZIO[Any, Nothing, (AnalyticsServiceLive, InMemoryAnalyticsStore, Ref[InMemoryAnalyticsState])] =
    for
      state <- Ref.make(InMemoryAnalyticsState())
      store = InMemoryAnalyticsStore(state)
      service = AnalyticsServiceLive(store)
    yield (service, store, state)

  private def at(date: LocalDate, hour: Int = 12): Instant =
    date.atTime(hour, 0).toInstant(ZoneOffset.UTC)

  private def admin(id: UUID = UUID.randomUUID()): AuthenticatedUser = AuthenticatedUser(UserId(id), Role.Admin)

  private def popugActor(id: UUID = UUID.randomUUID()): AuthenticatedUser = AuthenticatedUser(UserId(id), Role.Popug)

  private def userCreated(id: UUID, name: String, ts: Long): UserCreated =
    UserCreated(UUID.randomUUID().toString, ts, 1, id.toString, name, s"$name@popug.inc", "popug")

  private def taskCreated(id: UUID, title: String, fee: Long, reward: Long, ts: Long): TaskCreated =
    TaskCreated(UUID.randomUUID().toString, ts, 1, id.toString, title, "", id.toString, fee, reward)

  private def taskAssigned(taskId: UUID, assignee: UUID, fee: Long, ts: Long): TaskAssigned =
    TaskAssigned(UUID.randomUUID().toString, ts, 1, taskId.toString, "Task", assignee.toString, "", fee)

  private def taskCompleted(taskId: UUID, assignee: UUID, reward: Long, ts: Long): TaskCompleted =
    TaskCompleted(UUID.randomUUID().toString, ts, 1, taskId.toString, "Task", assignee.toString, reward)

  private def seedUser(store: InMemoryAnalyticsStore, id: UUID, name: String, ts: Long): ZIO[Any, Nothing, Unit] =
    AnalyticsEventProcessor.processUserCreated(userCreated(id, name, ts), store).orDieE

  private def seedTask(
      store: InMemoryAnalyticsStore,
      id: UUID,
      title: String,
      fee: Long,
      reward: Long,
      ts: Long
  ): ZIO[Any, Nothing, Unit] =
    AnalyticsEventProcessor.processTaskCreated(taskCreated(id, title, fee, reward, ts), store).orDieE

  /** Полный жизненный цикл задачи: создание, ассайн (списание fee), выполнение (начисление reward). */
  private def seedLifecycle(
      store: InMemoryAnalyticsStore,
      popug: UUID,
      taskId: UUID,
      fee: Long,
      reward: Long,
      createdTs: Long,
      assignedTs: Long,
      completedTs: Long
  ): ZIO[Any, Nothing, Unit] =
    for
      _ <- seedTask(store, taskId, "Task", fee, reward, createdTs)
      _ <- AnalyticsEventProcessor.processTaskAssigned(taskAssigned(taskId, popug, fee, assignedTs), store).orDieE
      _ <- AnalyticsEventProcessor.processTaskCompleted(taskCompleted(taskId, popug, reward, completedTs), store).orDieE
    yield ()

  def spec: Spec[Any, Any] =
    suite("AnalyticsService")(
      suite("M-ANL-06..07 top-management-earnings")(
        test("M-ANL-06 correct sum(assignFee - completeReward) per day over range") {
          for
            (service, store, _) <- makeService
            d1 = LocalDate.of(2026, 1, 15)
            d2 = LocalDate.of(2026, 1, 16)
            _ <- seedTask(store, UUID.randomUUID(), "A", 1500L, 2500L, at(d1).toEpochMilli)
            _ <- seedTask(store, UUID.randomUUID(), "B", 2000L, 4000L, at(d2).toEpochMilli)
            report <- service.topManagementEarnings(d1, d2, admin()).orDieE
          yield assertTrue(report.items.size == 2) &&
            assertTrue(report.items.head.date == d1) &&
            assertTrue(report.items.head.amount.toCents == -1000L) &&
            assertTrue(report.items.last.date == d2) &&
            assertTrue(report.items.last.amount.toCents == -2000L) &&
            assertTrue(report.total.toCents == -3000L)
        },
        test("M-ANL-07 no data in range -> empty items, zero total") {
          for
            (service, store, _) <- makeService
            _ <- seedTask(store, UUID.randomUUID(), "A", 1500L, 2500L, at(LocalDate.of(2026, 1, 10)).toEpochMilli)
            report <- service
              .topManagementEarnings(
                LocalDate.of(2026, 2, 1),
                LocalDate.of(2026, 2, 5),
                admin()
              )
              .orDieE
          yield assertTrue(report.items.isEmpty) && assertTrue(report.total.isZero)
        }
      ),
      suite("M-ANL-08..09 popugs-in-minus")(
        test("M-ANL-08 only negative balances are listed") {
          for
            (service, store, _) <- makeService
            minus = UUID.randomUUID()
            zero = UUID.randomUUID()
            positive = UUID.randomUUID()
            d = LocalDate.of(2026, 1, 15)
            _ <- seedUser(store, minus, "Minus", at(d).toEpochMilli)
            _ <- seedUser(store, zero, "Zero", at(d).toEpochMilli)
            _ <- seedUser(store, positive, "Positive", at(d).toEpochMilli)
            _ <- AnalyticsEventProcessor
              .processTaskAssigned(
                taskAssigned(UUID.randomUUID(), minus, 1500L, at(d).toEpochMilli),
                store
              )
              .orDieE
            _ <- seedLifecycle(
              store,
              positive,
              UUID.randomUUID(),
              1000L,
              3000L,
              at(d, 10).toEpochMilli,
              at(d, 11).toEpochMilli,
              at(d, 12).toEpochMilli
            )
            report <- service.popugsInMinus(admin()).orDieE
          yield assertTrue(report.count == 1) &&
            assertTrue(report.items.size == 1) &&
            assertTrue(report.items.head.userId == minus) &&
            assertTrue(report.items.head.name == "Minus") &&
            assertTrue(report.items.head.balanceCents == -1500L)
        },
        test("M-ANL-09 no negative popugs -> count=0, items empty (not an error)") {
          for
            (service, store, _) <- makeService
            d = LocalDate.of(2026, 1, 15)
            _ <- seedUser(store, UUID.randomUUID(), "Zero", at(d).toEpochMilli)
            report <- service.popugsInMinus(admin()).orDieE
          yield assertTrue(report.count == 0) && assertTrue(report.items.isEmpty)
        }
      ),
      suite("M-ANL-10..12 most-expensive-task")(
        test("M-ANL-10 period=day -> the most expensive closed task of the day") {
          for
            (service, store, _) <- makeService
            popug = UUID.randomUUID()
            d = LocalDate.of(2026, 1, 15)
            cheap = UUID.randomUUID()
            expensive = UUID.randomUUID()
            _ <- seedUser(store, popug, "Popug", at(d).toEpochMilli)
            _ <- seedLifecycle(
              store,
              popug,
              cheap,
              1000L,
              3000L,
              at(d, 10).toEpochMilli,
              at(d, 11).toEpochMilli,
              at(d, 12).toEpochMilli
            )
            _ <- seedLifecycle(
              store,
              popug,
              expensive,
              1000L,
              5000L,
              at(d, 13).toEpochMilli,
              at(d, 14).toEpochMilli,
              at(d, 15).toEpochMilli
            )
            report <- service.mostExpensiveTask(AnalyticsPeriod.Day, d, admin()).orDieE
          yield assertTrue(report.items.size == 1) &&
            assertTrue(report.items.head.date == d) &&
            assertTrue(report.items.head.task.taskId == expensive) &&
            assertTrue(report.items.head.task.amount.toCents == 5000L) &&
            assertTrue(report.overall.exists(_.taskId == expensive))
        },
        test("M-ANL-11 period=week -> one task per day + overall") {
          for
            (service, store, _) <- makeService
            popug = UUID.randomUUID()
            d1 = LocalDate.of(2026, 1, 15)
            d2 = LocalDate.of(2026, 1, 16)
            best = UUID.randomUUID()
            other = UUID.randomUUID()
            _ <- seedUser(store, popug, "Popug", at(d1).toEpochMilli)
            _ <- seedLifecycle(
              store,
              popug,
              best,
              1000L,
              9000L,
              at(d1, 10).toEpochMilli,
              at(d1, 11).toEpochMilli,
              at(d1, 12).toEpochMilli
            )
            _ <- seedLifecycle(
              store,
              popug,
              other,
              1000L,
              2000L,
              at(d2, 10).toEpochMilli,
              at(d2, 11).toEpochMilli,
              at(d2, 12).toEpochMilli
            )
            report <- service.mostExpensiveTask(AnalyticsPeriod.Week, d1, admin()).orDieE
          yield assertTrue(report.items.size == 2) &&
            assertTrue(report.items.head.date == d1) &&
            assertTrue(report.items.head.task.taskId == best) &&
            assertTrue(report.items.last.date == d2) &&
            assertTrue(report.items.last.task.taskId == other) &&
            assertTrue(report.overall.exists(_.taskId == best)) &&
            assertTrue(report.overall.exists(_.amount.toCents == 9000L))
        },
        test("M-ANL-12 no closed tasks in period -> empty items, overall = None") {
          for
            (service, store, _) <- makeService
            d = LocalDate.of(2026, 1, 15)
            _ <- seedTask(store, UUID.randomUUID(), "Open", 1000L, 3000L, at(d).toEpochMilli)
            report <- service.mostExpensiveTask(AnalyticsPeriod.Day, d, admin()).orDieE
          yield assertTrue(report.items.isEmpty) && assertTrue(report.overall.isEmpty)
        }
      ),
      suite("M-ANL-13..14 access")(
        test("M-ANL-13 non-admin -> AccessDenied (403)") {
          for
            (service, store, _) <- makeService
            d = LocalDate.of(2026, 1, 15)
            _ <- seedUser(store, UUID.randomUUID(), "Popug", at(d).toEpochMilli)
            earnings <- service.topManagementEarnings(d, d, popugActor()).either
            minus <- service.popugsInMinus(popugActor()).either
            expensive <- service.mostExpensiveTask(AnalyticsPeriod.Day, d, popugActor()).either
          yield assert(earnings)(isLeft(isSubtype[AccessDenied](anything))) &&
            assert(minus)(isLeft(isSubtype[AccessDenied](anything))) &&
            assert(expensive)(isLeft(isSubtype[AccessDenied](anything)))
        },
        test("M-ANL-14 admin -> all endpoints succeed") {
          for
            (service, _, _) <- makeService
            d = LocalDate.of(2026, 1, 15)
            earnings <- service.topManagementEarnings(d, d, admin()).orDieE
            minus <- service.popugsInMinus(admin()).orDieE
            expensive <- service.mostExpensiveTask(AnalyticsPeriod.Day, d, admin()).orDieE
          yield assertTrue(earnings.items.isEmpty) &&
            assertTrue(minus.items.isEmpty) &&
            assertTrue(expensive.items.isEmpty)
        }
      )
    )
