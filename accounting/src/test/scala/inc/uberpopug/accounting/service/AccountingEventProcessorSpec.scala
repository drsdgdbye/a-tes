package inc.uberpopug.accounting.service

import java.util.UUID

import zio.{Ref, ZIO}
import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.accounting.{InMemoryEventStore, InMemoryEventStoreState, orDieE}
import inc.uberpopug.accounting.domain.AccountEvent
import inc.uberpopug.common.domain.DomainError.AccountNotFound
import inc.uberpopug.common.domain.UserId

import auth.user_created.UserCreated
import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted
import task.task_created.TaskCreated

/** Тесты маппинга Kafka-событий в event store и идемпотентности (M-ACC-01..07). */
object AccountingEventProcessorSpec extends ZIOSpecDefault:
  private def makeState
      : ZIO[Any, Nothing, (AccountingEventProcessor.type, InMemoryEventStore, Ref[InMemoryEventStoreState])] =
    for
      state <- Ref.make(InMemoryEventStoreState())
      store = InMemoryEventStore(state)
    yield (AccountingEventProcessor, store, state)

  private def userCreated(id: UUID, name: String, ts: Long = 1000L, eventId: UUID = UUID.randomUUID()): UserCreated =
    UserCreated(eventId.toString, ts, version = 1, id.toString, name, s"$name@popug.inc", "popug")

  private def taskCreated(
      id: UUID,
      assigneeId: UUID,
      ts: Long = 2000L,
      fee: Long = 1500L,
      reward: Long = 2500L
  ): TaskCreated =
    TaskCreated(UUID.randomUUID().toString, ts, 1, id.toString, "Task", "", assigneeId.toString, fee, reward)

  private def taskAssigned(
      id: UUID,
      newAssignee: UUID,
      oldAssignee: Option[UUID],
      ts: Long = 2000L,
      fee: Long
  ): TaskAssigned =
    TaskAssigned(
      UUID.randomUUID().toString,
      ts,
      1,
      id.toString,
      "Task",
      newAssignee.toString,
      oldAssignee.map(_.toString).getOrElse(""),
      fee
    )

  private def taskCompleted(
      id: UUID,
      assigneeId: UUID,
      ts: Long = 2000L,
      reward: Long
  ): TaskCompleted =
    TaskCompleted(UUID.randomUUID().toString, ts, 1, id.toString, "Task", assigneeId.toString, reward)

  private def registerUser(
      processor: AccountingEventProcessor.type,
      store: InMemoryEventStore,
      id: UUID
  ): ZIO[Any, Nothing, Unit] =
    processor.processUserCreated(userCreated(id, s"popug-$id"), store).orDieE

  def spec: Spec[Any, Any] =
    suite("AccountingEventProcessor")(
      suite("M-ACC-01..04 event mapping")(
        test("M-ACC-01 TaskCreated -> TaskPriceRecorded with prices") {
          for
            (processor, store, state) <- makeState
            userId = UUID.randomUUID()
            taskId = UUID.randomUUID()
            _ <- registerUser(processor, store, userId)
            _ <- processor.processTaskCreated(taskCreated(taskId, userId), store).orDieE
            events <- state.get.map(_.events)
            recorded = events.collect { case e: AccountEvent.TaskPriceRecorded => e }
          yield assertTrue(recorded.size == 1) &&
            assertTrue(recorded.head.taskId.value == taskId) &&
            assertTrue(recorded.head.assignFee.toCents == 1500L) &&
            assertTrue(recorded.head.completeReward.toCents == 2500L) &&
            assertTrue(recorded.head.userId.value == userId)
        },
        test("M-ACC-02 TaskAssigned first assign -> single AccountDebited from new assignee") {
          for
            (processor, store, state) <- makeState
            userId = UUID.randomUUID()
            taskId = UUID.randomUUID()
            _ <- registerUser(processor, store, userId)
            _ <- processor.processTaskAssigned(taskAssigned(taskId, userId, None, fee = 1500L), store).orDieE
            events <- state.get.map(_.events)
            debits = events.collect { case e: AccountEvent.AccountDebited => e }
            refunds = events.collect { case e: AccountEvent.AccountCredited => e }
          yield assertTrue(debits.size == 1, refunds.isEmpty) &&
            assertTrue(debits.head.userId.value == userId) &&
            assertTrue(debits.head.amount.toCents == 1500L)
        },
        test("M-ACC-03 TaskAssigned reassign -> refund to old + debit from new") {
          for
            (processor, store, state) <- makeState
            oldId = UUID.randomUUID()
            newId = UUID.randomUUID()
            taskId = UUID.randomUUID()
            _ <- registerUser(processor, store, oldId)
            _ <- registerUser(processor, store, newId)
            _ <- processor.processTaskAssigned(taskAssigned(taskId, newId, Some(oldId), fee = 1500L), store).orDieE
            events <- state.get.map(_.events)
            refunds = events.collect { case e: AccountEvent.AccountCredited => e }
            debits = events.collect { case e: AccountEvent.AccountDebited => e }
          yield assertTrue(refunds.size == 1, debits.size == 1) &&
            assertTrue(refunds.head.userId.value == oldId) &&
            assertTrue(debits.head.userId.value == newId) &&
            assertTrue(refunds.head.reason == AccountEvent.CreditReason.AssignmentRefund)
        },
        test("M-ACC-04 TaskCompleted -> AccountCredited completeReward") {
          for
            (processor, store, state) <- makeState
            userId = UUID.randomUUID()
            taskId = UUID.randomUUID()
            _ <- registerUser(processor, store, userId)
            _ <- processor.processTaskCompleted(taskCompleted(taskId, userId, reward = 3000L), store).orDieE
            events <- state.get.map(_.events)
            credits = events.collect { case e: AccountEvent.AccountCredited => e }
          yield assertTrue(credits.size == 1) &&
            assertTrue(credits.head.amount.toCents == 3000L) &&
            assertTrue(credits.head.reason == AccountEvent.CreditReason.TaskCompleted)
        }
      ),
      suite("M-ACC-05..07 dedup / poison / ordering")(
        test("M-ACC-05 duplicate event_id is ignored (idempotent)") {
          for
            (processor, store, state) <- makeState
            userId = UUID.randomUUID()
            taskId = UUID.randomUUID()
            _ <- registerUser(processor, store, userId)
            event = taskCreated(taskId, userId)
            _ <- processor.processTaskCreated(event, store).orDieE
            _ <- processor.processTaskCreated(event, store).orDieE
            events <- state.get.map(_.events)
          yield assertTrue(events.count(_.isInstanceOf[AccountEvent.TaskPriceRecorded]) == 1)
        },
        test("M-ACC-06 event for unknown account -> AccountNotFound (poison -> DLQ)") {
          for
            (processor, store, _) <- makeState
            unknown = UUID.randomUUID()
            result <- processor.processTaskCreated(taskCreated(UUID.randomUUID(), unknown), store).either
          yield assert(result)(isLeft(isSubtype[AccountNotFound](anything)))
        },
        test("M-ACC-07 out-of-order events produce correct balance") {
          for
            (processor, store, _) <- makeState
            userId = UUID.randomUUID()
            taskId = UUID.randomUUID()
            _ <- registerUser(processor, store, userId)
            _ <- processor.processTaskCompleted(taskCompleted(taskId, userId, ts = 5000L, reward = 2500L), store).orDieE
            _ <- processor
              .processTaskAssigned(taskAssigned(taskId, userId, None, ts = 1000L, fee = 1500L), store)
              .orDieE
            balance <- store.balanceOf(UserId(userId)).orDieE
          yield assertTrue(balance.exists(_.toCents == 1000L))
        }
      )
    )
