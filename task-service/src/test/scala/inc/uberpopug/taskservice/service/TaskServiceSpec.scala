package inc.uberpopug.taskservice.service

import java.time.Instant
import java.util.UUID

import scala.util.Random

import zio.{Chunk, Clock, Ref, ZIO, ZLayer}
import zio.test.Assertion.*
import zio.test.*

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.*
import inc.uberpopug.common.domain.{Money, TaskId, UserId}
import inc.uberpopug.taskservice.domain.*
import inc.uberpopug.taskservice.repository.{OutboxRecord, TaskRepository}

/** In-memory состояние репозитория задач: задачи и накопленный outbox. */
final case class TaskRepoState(tasks: Map[UUID, Task] = Map.empty, outbox: Chunk[OutboxRecord] = Chunk.empty)

/** In-memory реализация TaskRepository на Ref с честным optimistic lock. */
final case class TaskRepoInMemory(state: Ref[TaskRepoState]) extends TaskRepository:
  def createWithOutbox(task: Task, outbox: List[OutboxRecord]): ZIO[Any, DomainError, Unit] =
    state.update(s => s.copy(tasks = s.tasks + (task.id.value -> task), outbox = s.outbox ++ outbox))

  def findById(id: TaskId): ZIO[Any, DomainError, Option[Task]] =
    state.get.map(_.tasks.get(id.value))

  def listByAssignee(assigneeId: UserId, limit: Int, offset: Int): ZIO[Any, DomainError, List[Task]] =
    state.get.map(
      _.tasks.values.filter(_.assigneeId == assigneeId).toList.sortBy(_.createdAt).slice(offset, offset + limit)
    )

  def countByAssignee(assigneeId: UserId): ZIO[Any, DomainError, Long] =
    state.get.map(_.tasks.values.count(_.assigneeId == assigneeId).toLong)

  def listAll(limit: Int, offset: Int): ZIO[Any, DomainError, List[Task]] =
    state.get.map(_.tasks.values.toList.sortBy(_.createdAt).slice(offset, offset + limit))

  def countAll: ZIO[Any, DomainError, Long] =
    state.get.map(_.tasks.size.toLong)

  def listOpen(limit: Int, offset: Int): ZIO[Any, DomainError, List[Task]] =
    state.get.map(
      _.tasks.values.filter(_.status == TaskStatus.Open).toList.sortBy(_.createdAt).slice(offset, offset + limit)
    )

  def completeWithOutbox(completed: Task, expectedVersion: Long, outbox: OutboxRecord): ZIO[Any, DomainError, Unit] =
    state
      .modify { s =>
        s.tasks.get(completed.id.value) match
          case Some(current) if current.version == expectedVersion && current.status == TaskStatus.Open =>
            (true, s.copy(tasks = s.tasks + (completed.id.value -> completed), outbox = s.outbox :+ outbox))
          case _ => (false, s)
      }
      .flatMap { applied =>
        if applied then ZIO.unit
        else ZIO.fail(OptimisticLockConflict(s"Task ${completed.id.value} changed concurrently"))
      }

  def reassignWithOutbox(
      reassigned: Task,
      expectedVersion: Long,
      outbox: OutboxRecord
  ): ZIO[Any, DomainError, Boolean] =
    state.modify { s =>
      s.tasks.get(reassigned.id.value) match
        case Some(current) if current.version == expectedVersion && current.status == TaskStatus.Open =>
          (true, s.copy(tasks = s.tasks + (reassigned.id.value -> reassigned), outbox = s.outbox :+ outbox))
        case _ => (false, s)
    }

/** Тесты TaskService: создание, списки, завершение (права + optimistic lock), перетасовка (права). */
object TaskServiceSpec extends ZIOSpecDefault:
  private val now = Instant.parse("2026-01-01T00:00:00Z")

  private def userId(): UserId = UserId(UUID.randomUUID())
  private def popug(): AuthenticatedUser = AuthenticatedUser(userId(), Role.Popug)
  private def manager(): AuthenticatedUser = AuthenticatedUser(userId(), Role.Manager)

  /** Снижает env-требование эффекта с `Clock` до `Any` (см. AGENTS, ключевое решение auth). */
  private def withLiveClock[E, A](effect: ZIO[Clock, E, A]): ZIO[Any, E, A] =
    effect.provideLayer(ZLayer.succeed(Clock.ClockLive))

  private def makeService(
      popugs: Set[UserId]
  ): ZIO[Any, Nothing, (TaskService, Ref[TaskRepoState])] =
    for
      state <- Ref.make(TaskRepoState())
      eligibleRef <- Ref.make(popugs)
      service = TaskServiceLive(TaskRepoInMemory(state), EligiblePopugs(eligibleRef), Random(42))
    yield (service, state)

  private def seedTask(state: Ref[TaskRepoState], assignee: UserId, title: String): ZIO[Any, DomainError, Unit] =
    for
      task <- ZIO.fromEither(
        Task.create(
          TaskId(UUID.randomUUID()),
          TaskTitle(title),
          None,
          assignee,
          Money.fromCents(1500L),
          Money.fromCents(2500L),
          now
        )
      )
      _ <- state.update(s => s.copy(tasks = s.tasks + (task.id.value -> task)))
    yield ()

  def spec: Spec[Any, Any] =
    suite("TaskService")(
      suite("createTask")(
        test("creates a task assigned to an eligible popug with outbox records") {
          withLiveClock {
            for
              actor = popug()
              (service, state) <- makeService(Set(actor.id))
              task <- service.createTask("Write tests", Some("   "), actor)
              outboxTypes <- state.get.map(_.outbox.map(_.eventType).toList)
            yield assertTrue(
              task.status == TaskStatus.Open,
              task.assigneeId == actor.id,
              task.title.value == "Write tests",
              task.description.isEmpty,
              task.assignFee >= Task.MinAssignFee && task.assignFee <= Task.MaxAssignFee,
              task.completeReward >= Task.MinCompleteReward && task.completeReward <= Task.MaxCompleteReward,
              outboxTypes == List(TaskEventTypes.TaskCreated, TaskEventTypes.TaskAssigned)
            )
          }
        },
        test("rejects an empty title") {
          withLiveClock {
            for
              actor = popug()
              (service, _) <- makeService(Set(actor.id))
              result <- service.createTask("   ", None, actor).exit
            yield assert(result)(fails(isSubtype[InvalidValue](anything)))
          }
        },
        test("fails when there are no eligible popugs") {
          withLiveClock {
            for
              actor = popug()
              (service, _) <- makeService(Set.empty)
              result <- service.createTask("Lonely task", None, actor).exit
            yield assert(result)(fails(isSubtype[BusinessRuleViolation](anything)))
          }
        }
      ),
      suite("completeTask")(
        test("completes an open task by its assignee and writes TaskCompleted") {
          withLiveClock {
            for
              actor = popug()
              (service, state) <- makeService(Set(actor.id))
              task <- service.createTask("Done soon", None, actor)
              completed <- service.completeTask(task.id, actor)
              outboxTypes <- state.get.map(_.outbox.map(_.eventType).toList)
            yield assertTrue(
              completed.status == TaskStatus.Completed,
              completed.completedAt.nonEmpty,
              completed.version == task.version + 1,
              outboxTypes.last == TaskEventTypes.TaskCompleted
            )
          }
        },
        test("rejects completing by a non-assignee") {
          withLiveClock {
            for
              owner = popug()
              (service, _) <- makeService(Set(owner.id))
              task <- service.createTask("Not yours", None, owner)
              stranger = popug()
              result <- service.completeTask(task.id, stranger).exit
            yield assert(result)(fails(equalTo(AccessDenied("Only the assignee can complete the task"))))
          }
        },
        test("returns TaskNotFound for an unknown task") {
          withLiveClock {
            for
              actor = popug()
              (service, _) <- makeService(Set(actor.id))
              result <- service.completeTask(TaskId(UUID.randomUUID()), actor).exit
            yield assert(result)(fails(isSubtype[TaskNotFound](anything)))
          }
        },
        test("fails with OptimisticLockConflict when the stored version differs from the expected one") {
          for
            actor = popug()
            (_, state) <- makeService(Set(actor.id))
            _ <- seedTask(state, actor.id, "Race me")
            stored <- state.get.map(_.tasks.values.head)
            repo = TaskRepoInMemory(state)
            outbox = OutboxRecord(stored.id.value, "TaskCompleted", Array.emptyByteArray, now)
            result <- repo.completeWithOutbox(stored, expectedVersion = stored.version + 5, outbox).exit
          yield assert(result)(fails(isSubtype[OptimisticLockConflict](anything)))
        }
      ),
      suite("shuffle")(
        test("reassigns each open task to a different popug for a manager") {
          withLiveClock {
            for
              first = popug()
              second = popug()
              (service, state) <- makeService(Set(first.id, second.id))
              task <- service.createTask("Shuffle me", None, first)
              originalAssignee = task.assigneeId
              count <- service.shuffle(manager())
              updated <- state.get.map(_.tasks(task.id.value))
              outboxTypes <- state.get.map(_.outbox.map(_.eventType).toList)
            yield assertTrue(
              count == 1,
              updated.assigneeId != originalAssignee,
              updated.version == task.version + 1,
              outboxTypes.last == TaskEventTypes.TaskAssigned
            )
          }
        },
        test("returns zero when fewer than two popugs are eligible") {
          withLiveClock {
            for
              actor = popug()
              (service, _) <- makeService(Set(actor.id))
              _ <- service.createTask("Alone", None, actor)
              count <- service.shuffle(manager())
            yield assertTrue(count == 0)
          }
        },
        test("returns zero when there are no open tasks (M-TASK-20)") {
          withLiveClock {
            for
              first = popug()
              second = popug()
              (service, _) <- makeService(Set(first.id, second.id))
              count <- service.shuffle(manager())
            yield assertTrue(count == 0)
          }
        },
        test("skips non-open tasks and returns zero (M-TASK-22)") {
          withLiveClock {
            for
              actor = popug()
              (service, state) <- makeService(Set(actor.id))
              task <- service.createTask("Completed", None, actor)
              _ <- service.completeTask(task.id, actor)
              count <- service.shuffle(manager())
              updated <- state.get.map(_.tasks(task.id.value))
            yield assertTrue(count == 0, updated.status == TaskStatus.Completed)
          }
        },
        test("keeps a disabled popug as assignee of open tasks until shuffle (M-TASK-23)") {
          withLiveClock {
            for
              disabledId = userId()
              other = popug()
              another = popug()
              (service, state) <- makeService(Set(other.id, another.id))
              _ <- seedTask(state, disabledId, "Held by disabled")
              task <- state.get.map(_.tasks.values.head)
              before <- service.getTask(task.id, manager())
              count <- service.shuffle(manager())
              updated <- state.get.map(_.tasks(task.id.value))
            yield assertTrue(before.assigneeId == disabledId, count == 1, updated.assigneeId != disabledId)
          }
        },
        test("writes TaskAssigned with old and new assignee ids in outbox (M-TASK-25)") {
          withLiveClock {
            for
              first = popug()
              second = popug()
              (service, state) <- makeService(Set(first.id, second.id))
              created <- service.createTask("Shuffle me", None, first)
              originalAssignee = created.assigneeId
              _ <- service.shuffle(manager())
              updated <- state.get.map(_.tasks(created.id.value))
              record <- state.get.map(_.outbox.filter(_.eventType == TaskEventTypes.TaskAssigned).last)
              event = _root_.task.task_assigned.TaskAssigned.parseFrom(record.payload)
            yield assertTrue(
              event.oldAssigneeId == originalAssignee.value.toString,
              event.newAssigneeId == updated.assigneeId.value.toString,
              event.newAssigneeId != originalAssignee.value.toString
            )
          }
        },
        test("rejects shuffle for a popug") {
          withLiveClock {
            for
              actor = popug()
              (service, _) <- makeService(Set(actor.id))
              result <- service.shuffle(actor).exit
            yield assert(result)(fails(isSubtype[AccessDenied](anything)))
          }
        }
      ),
      suite("getTask")(
        test("returns the task by id") {
          for
            actor = popug()
            (service, state) <- makeService(Set(actor.id))
            _ <- seedTask(state, actor.id, "Seed task")
            taskId <- state.get.map(_.tasks.head._1)
            found <- service.getTask(TaskId(taskId), actor)
          yield assertTrue(found.title.value == "Seed task")
        },
        test("returns TaskNotFound for an unknown id") {
          for
            actor = popug()
            (service, _) <- makeService(Set(actor.id))
            result <- service.getTask(TaskId(UUID.randomUUID()), actor).exit
          yield assert(result)(fails(isSubtype[TaskNotFound](anything)))
        }
      ),
      suite("listAllTasks")(
        test("returns all tasks with pagination") {
          for
            actor = popug()
            (service, state) <- makeService(Set(actor.id))
            _ <- seedTask(state, actor.id, "First")
            _ <- seedTask(state, actor.id, "Second")
            (items, total) <- service.listAllTasks(1, 1, actor)
          yield assertTrue(total == 2L, items.size == 1, items.head.title.value == "Second")
        }
      )
    )
