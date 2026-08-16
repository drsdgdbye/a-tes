package inc.uberpopug.taskservice.domain

import java.time.Instant

import inc.uberpopug.common.domain.{DomainError, Money, TaskId, UserId}
import inc.uberpopug.common.domain.DomainError.{BusinessRuleViolation, InvalidValue}

/** Задача — корневая сущность Task Service (aggregate root). Всегда имеет исполнителя. */
final case class Task(
    id: TaskId,
    title: TaskTitle,
    description: Option[TaskDescription],
    status: TaskStatus,
    assigneeId: UserId,
    assignFee: Money,
    completeReward: Money,
    createdAt: Instant,
    completedAt: Option[Instant],
    version: Long
)

object Task:
  /** Минимальный assignFee: `$10.00`. */
  val MinAssignFee = Money.fromCents(1000L)

  /** Максимальный assignFee: `$20.00`. */
  val MaxAssignFee = Money.fromCents(2000L)

  /** Минимальный completeReward: `$20.00`. */
  val MinCompleteReward = Money.fromCents(2000L)

  /** Максимальный completeReward: `$40.00`. */
  val MaxCompleteReward = Money.fromCents(4000L)

  /** Стартовая версия для optimistic lock. */
  val InitialVersion = 1L

  /** Создаёт задачу со статусом `Open`, без `completedAt` и со стартовой версией. Цены валидируются по доменным
    * диапазонам.
    */
  def create(
      id: TaskId,
      title: TaskTitle,
      description: Option[TaskDescription],
      assigneeId: UserId,
      assignFee: Money,
      completeReward: Money,
      now: Instant
  ): Either[DomainError, Task] =
    for
      _ <- validatePrice("assignFee", assignFee, MinAssignFee, MaxAssignFee)
      _ <- validatePrice("completeReward", completeReward, MinCompleteReward, MaxCompleteReward)
    yield Task(
      id,
      title,
      description,
      TaskStatus.Open,
      assigneeId,
      assignFee,
      completeReward,
      now,
      None,
      InitialVersion
    )

  /** Переводит задачу в статус `Completed` с моментом выполнения и инкрементом версии. Обратный переход запрещён. */
  def complete(task: Task, now: Instant): Either[DomainError, Task] =
    if task.status != TaskStatus.Open then Left(BusinessRuleViolation(s"Task ${task.id.value} is not open"))
    else Right(task.copy(status = TaskStatus.Completed, completedAt = Some(now), version = task.version + 1))

  /** Меняет исполнителя (реассайн) и инкрементирует версию. Гарантию `newAssigneeId != oldAssigneeId` обеспечивает
    * `AssignmentPolicy.shuffle`.
    */
  def reassign(task: Task, newAssigneeId: UserId): Task =
    task.copy(assigneeId = newAssigneeId, version = task.version + 1)

  /** Проверяет, что цена попадает в доменный диапазон (включительно). */
  private def validatePrice(field: String, value: Money, min: Money, max: Money): Either[DomainError, Unit] =
    if value >= min && value <= max then Right(())
    else Left(InvalidValue(field, s"$field must be between ${min.value} and ${max.value}, got: ${value.value}"))
