package inc.uberpopug.analytics.service

import java.time.LocalDate
import java.util.UUID

import zio.{Ref, ZIO}
import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.analytics.{InMemoryAnalyticsState, InMemoryAnalyticsStore, orDieE}
import inc.uberpopug.common.domain.DomainError.AccountNotFound

import accounting.payment_processed.PaymentProcessed
import auth.user_created.UserCreated
import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted
import task.task_created.TaskCreated

/** Тесты обработки Kafka-событий в read-side проекции и идемпотентности (M-ANL-01..05). */
object AnalyticsEventProcessorSpec extends ZIOSpecDefault:
  private def makeState
      : ZIO[Any, Nothing, (AnalyticsEventProcessor.type, InMemoryAnalyticsStore, Ref[InMemoryAnalyticsState])] =
    for
      state <- Ref.make(InMemoryAnalyticsState())
      store = InMemoryAnalyticsStore(state)
    yield (AnalyticsEventProcessor, store, state)

  private def userCreated(id: UUID, name: String, ts: Long = 1000L, eventId: UUID = UUID.randomUUID()): UserCreated =
    UserCreated(eventId.toString, ts, version = 1, id.toString, name, s"$name@popug.inc", "popug")

  private def taskCreated(
      id: UUID,
      title: String = "Task",
      fee: Long = 1500L,
      reward: Long = 2500L,
      ts: Long = 2000L,
      eventId: UUID = UUID.randomUUID()
  ): TaskCreated =
    TaskCreated(eventId.toString, ts, 1, id.toString, title, "", id.toString, fee, reward)

  private def taskAssigned(
      taskId: UUID,
      newAssignee: UUID,
      oldAssignee: Option[UUID],
      fee: Long,
      ts: Long = 2000L,
      eventId: UUID = UUID.randomUUID()
  ): TaskAssigned =
    TaskAssigned(
      eventId.toString,
      ts,
      1,
      taskId.toString,
      "Task",
      newAssignee.toString,
      oldAssignee.map(_.toString).getOrElse(""),
      fee
    )

  private def taskCompleted(
      taskId: UUID,
      assigneeId: UUID,
      reward: Long,
      ts: Long,
      eventId: UUID = UUID.randomUUID()
  ): TaskCompleted =
    TaskCompleted(eventId.toString, ts, 1, taskId.toString, "Task", assigneeId.toString, reward)

  private def paymentProcessed(
      popugId: UUID,
      name: String,
      amount: Long,
      date: LocalDate,
      ts: Long = 4000L,
      eventId: UUID = UUID.randomUUID()
  ): PaymentProcessed =
    PaymentProcessed(eventId.toString, ts, 1, popugId.toString, name, amount, date.toString)

  private def registerUser(
      processor: AnalyticsEventProcessor.type,
      store: InMemoryAnalyticsStore,
      id: UUID
  ): ZIO[Any, Nothing, Unit] =
    processor.processUserCreated(userCreated(id, s"popug-$id"), store).orDieE

  def spec: Spec[Any, Any] =
    suite("AnalyticsEventProcessor")(
      suite("M-ANL-01..05 projections")(
        test("M-ANL-01 TaskCreated -> INSERT tasks with all fields") {
          for
            (processor, store, state) <- makeState
            taskId = UUID.randomUUID()
            _ <- processor
              .processTaskCreated(taskCreated(taskId, title = "Fix bug", fee = 1200L, reward = 2800L), store)
              .orDieE
            tasks <- state.get.map(_.tasks)
            row = tasks(taskId)
          yield assertTrue(row.title == "Fix bug") &&
            assertTrue(row.assignFeeCents == 1200L) &&
            assertTrue(row.completeRewardCents == 2800L) &&
            assertTrue(row.status == "open") &&
            assertTrue(row.completedAt.isEmpty)
        },
        test("M-ANL-02 TaskCompleted -> UPDATE tasks.status + completedAt + balance credit") {
          for
            (processor, store, state) <- makeState
            popug = UUID.randomUUID()
            taskId = UUID.randomUUID()
            _ <- registerUser(processor, store, popug)
            _ <- processor.processTaskCreated(taskCreated(taskId), store).orDieE
            _ <- processor.processTaskCompleted(taskCompleted(taskId, popug, reward = 3000L, ts = 5000L), store).orDieE
            tasks <- state.get.map(_.tasks)
            popugs <- state.get.map(_.popugs)
          yield assertTrue(tasks(taskId).status == "completed") &&
            assertTrue(tasks(taskId).completedAt.isDefined) &&
            assertTrue(popugs(popug).balanceCents == 3000L)
        },
        test("M-ANL-03 TaskAssigned -> debit new assignee, credit old (refund)") {
          for
            (processor, store, state) <- makeState
            oldId = UUID.randomUUID()
            newId = UUID.randomUUID()
            taskId = UUID.randomUUID()
            _ <- registerUser(processor, store, oldId)
            _ <- registerUser(processor, store, newId)
            _ <- processor.processTaskAssigned(taskAssigned(taskId, newId, Some(oldId), fee = 1500L), store).orDieE
            popugs <- state.get.map(_.popugs)
          yield assertTrue(popugs(newId).balanceCents == -1500L) &&
            assertTrue(popugs(oldId).balanceCents == 1500L)
        },
        test("M-ANL-04 PaymentProcessed -> balance -= amount, daily_stats.popugs_negative") {
          for
            (processor, store, state) <- makeState
            popug = UUID.randomUUID()
            _ <- registerUser(processor, store, popug)
            date = LocalDate.of(2026, 1, 15)
            _ <- processor
              .processPaymentProcessed(paymentProcessed(popug, "popug-1", amount = 0L, date = date), store)
              .orDieE
            popugs <- state.get.map(_.popugs)
            stats <- state.get.map(_.dailyStats)
          yield assertTrue(popugs(popug).balanceCents == 0L) &&
            assertTrue(stats(date).popugsNegative == 0)
        },
        test("M-ANL-05 duplicate event_id is ignored (idempotent)") {
          for
            (processor, store, state) <- makeState
            popug = UUID.randomUUID()
            taskId = UUID.randomUUID()
            _ <- registerUser(processor, store, popug)
            created = taskCreated(taskId)
            _ <- processor.processTaskCreated(created, store).orDieE
            _ <- processor.processTaskCreated(created, store).orDieE
            tasks <- state.get.map(_.tasks)
            processed <- state.get.map(_.processed)
          yield assertTrue(tasks.size == 1) &&
            assertTrue(processed.size == 2)
        }
      ),
      suite("ordering / transient")(
        test("event for unknown popug -> AccountNotFound (transient, not poison)") {
          for
            (processor, store, _) <- makeState
            unknown = UUID.randomUUID()
            result <- processor
              .processTaskAssigned(taskAssigned(UUID.randomUUID(), unknown, None, fee = 1500L), store)
              .either
          yield assert(result)(isLeft(isSubtype[AccountNotFound](anything)))
        },
        test("out-of-order events produce correct balance (completed before assigned)") {
          for
            (processor, store, state) <- makeState
            popug = UUID.randomUUID()
            taskId = UUID.randomUUID()
            _ <- registerUser(processor, store, popug)
            _ <- processor.processTaskCompleted(taskCompleted(taskId, popug, reward = 3000L, ts = 5000L), store).orDieE
            _ <- processor.processTaskAssigned(taskAssigned(taskId, popug, None, fee = 1500L, ts = 1000L), store).orDieE
            balance <- state.get.map(_.popugs(popug).balanceCents)
          yield assertTrue(balance == 1500L)
        }
      )
    )
