package inc.uberpopug.taskservice.domain

import java.time.Instant
import java.util.UUID

import scala.util.Random

import zio.test.Assertion.*
import zio.test.*

import inc.uberpopug.common.domain.{Money, TaskId, UserId}
import inc.uberpopug.common.domain.DomainError.{BusinessRuleViolation, InvalidValue}

/** Тесты доменной модели Task Service: smart-конструкторы, Task, ценовая и ассайн-политики. */
object DomainSpec extends ZIOSpecDefault:
  private val now = Instant.parse("2026-01-01T00:00:00Z")
  private val popug1 = UserId(UUID.randomUUID())
  private val popug2 = UserId(UUID.randomUUID())
  private val taskId = TaskId(UUID.randomUUID())

  private def validTask(
      assignFee: Money = Money.fromCents(1500L),
      completeReward: Money = Money.fromCents(2500L)
  ): Task =
    Task.create(taskId, TaskTitle("Do the thing"), None, popug1, assignFee, completeReward, now).toOption.get

  def spec: Spec[Any, Any] =
    suite("Task Service domain")(
      suite("TaskTitle.from")(
        test("accepts a non-empty title and trims whitespace") {
          assertTrue(TaskTitle.from("  Fix bugs  ") == Right(TaskTitle("Fix bugs")))
        },
        test("rejects an empty title") {
          assert(TaskTitle.from("   "))(isLeft(equalTo(InvalidValue("title", "Task title must not be empty"))))
        },
        test("rejects a title longer than 500 characters") {
          assert(TaskTitle.from("x" * 501))(isLeft(isSubtype[InvalidValue](anything)))
        },
        test("accepts a title of exactly 500 characters") {
          assertTrue(TaskTitle.from("x" * 500).isRight)
        }
      ),
      suite("TaskDescription.from")(
        test("accepts a non-empty description") {
          assertTrue(TaskDescription.from("  details  ") == Right(TaskDescription("details")))
        },
        test("rejects an empty description") {
          assert(TaskDescription.from(" "))(
            isLeft(equalTo(InvalidValue("description", "Task description must not be empty")))
          )
        }
      ),
      suite("TaskStatus.from")(
        test("parses known statuses") {
          assertTrue(
            TaskStatus.from("open") == Right(TaskStatus.Open),
            TaskStatus.from("completed") == Right(TaskStatus.Completed)
          )
        },
        test("rejects unknown statuses") {
          assert(TaskStatus.from("deleted"))(isLeft(equalTo(InvalidValue("status", "Invalid status: 'deleted'"))))
        }
      ),
      suite("Role.from")(
        test("parses known roles") {
          assertTrue(
            Role.from("popug") == Right(Role.Popug),
            Role.from("manager") == Right(Role.Manager),
            Role.from("accountant") == Right(Role.Accountant),
            Role.from("admin") == Right(Role.Admin)
          )
        },
        test("rejects unknown roles") {
          assert(Role.from("ceo"))(isLeft(isSubtype[InvalidValue](anything)))
        }
      ),
      suite("Task.create")(
        test("creates an open task with initial version and no completedAt") {
          val task = validTask()
          assertTrue(
            task.status == TaskStatus.Open,
            task.assigneeId == popug1,
            task.completedAt.isEmpty,
            task.version == Task.InitialVersion,
            task.assignFee == Money.fromCents(1500L),
            task.completeReward == Money.fromCents(2500L)
          )
        },
        test("rejects an assign fee below the domain range") {
          assert(Task.create(taskId, TaskTitle("T"), None, popug1, Money.fromCents(999L), Money.fromCents(2500L), now))(
            isLeft(isSubtype[InvalidValue](anything))
          )
        },
        test("rejects a complete reward above the domain range") {
          assert(
            Task.create(taskId, TaskTitle("T"), None, popug1, Money.fromCents(1500L), Money.fromCents(4001L), now)
          )(
            isLeft(isSubtype[InvalidValue](anything))
          )
        }
      ),
      suite("Task.complete")(
        test("completes an open task with a timestamp and version bump") {
          val completed = Task.complete(validTask(), now).toOption.get
          assertTrue(
            completed.status == TaskStatus.Completed,
            completed.completedAt.contains(now),
            completed.version == Task.InitialVersion + 1
          )
        },
        test("rejects completing an already completed task") {
          val completed = Task.complete(validTask(), now).toOption.get
          assert(Task.complete(completed, now))(isLeft(isSubtype[BusinessRuleViolation](anything)))
        }
      ),
      suite("Task.reassign")(
        test("changes the assignee and bumps the version") {
          val reassigned = Task.reassign(validTask(), popug2)
          assertTrue(reassigned.assigneeId == popug2, reassigned.version == Task.InitialVersion + 1)
        }
      ),
      suite("PricingPolicy")(
        test("generates assign fees within [10, 20] with a 1.00 step") {
          val fees = (1 to 100).map(_ => PricingPolicy.generateAssignFee(Random(42))).toSet
          assertTrue(
            fees.forall(f => f >= Task.MinAssignFee && f <= Task.MaxAssignFee),
            fees.forall(f => f.toCents % 100 == 0)
          )
        },
        test("generates complete rewards within [20, 40] with a 1.00 step") {
          val rewards = (1 to 100).map(_ => PricingPolicy.generateCompleteReward(Random(42))).toSet
          assertTrue(
            rewards.forall(r => r >= Task.MinCompleteReward && r <= Task.MaxCompleteReward),
            rewards.forall(r => r.toCents % 100 == 0)
          )
        }
      ),
      suite("AssignmentPolicy.assignRandom")(
        test("returns a member of the eligible list") {
          val result = AssignmentPolicy.assignRandom(List(popug1, popug2), Random(7))
          assertTrue(result.toOption.exists(p => p == popug1 || p == popug2))
        },
        test("fails when no popugs are eligible") {
          assert(AssignmentPolicy.assignRandom(Nil, Random(7)))(isLeft(isSubtype[BusinessRuleViolation](anything)))
        }
      ),
      suite("AssignmentPolicy.shuffle")(
        test("returns no reassignments when fewer than two popugs are available") {
          assertTrue(AssignmentPolicy.shuffle(List(validTask()), List(popug1), Random(7)) == Nil)
        },
        test("never reassigns a task to its current assignee") {
          val tasks = List(validTask(), validTask().copy(assigneeId = popug2))
          val reassignments = AssignmentPolicy.shuffle(tasks, List(popug1, popug2), Random(42))
          assertTrue(
            reassignments.nonEmpty,
            reassignments.forall(r => r.newAssigneeId != r.oldAssigneeId),
            reassignments.forall(r => r.taskId == taskId)
          )
        }
      )
    )
