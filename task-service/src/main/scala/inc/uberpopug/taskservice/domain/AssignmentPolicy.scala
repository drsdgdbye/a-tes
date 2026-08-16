package inc.uberpopug.taskservice.domain

import scala.util.Random

import inc.uberpopug.common.domain.{DomainError, TaskId, UserId}
import inc.uberpopug.common.domain.DomainError.BusinessRuleViolation

/** Результат перераспределения: задаче меняется исполнитель. */
final case class Reassignment(taskId: TaskId, oldAssigneeId: UserId, newAssigneeId: UserId)

/** Чистые функции политики назначения: выбор исполнителя для новой задачи и «перетасовка» задач между сотрудниками. */
object AssignmentPolicy:
  /** Выбирает случайного сотрудника из списка доступных. Пустой список — бизнес-ошибка. */
  def assignRandom(eligible: List[UserId], random: Random): Either[DomainError, UserId] =
    eligible match
      case Nil  => Left(BusinessRuleViolation("No eligible popugs available for assignment"))
      case list => Right(list(random.nextInt(list.size)))

  /** Перераспределяет задачи: для каждой задачи выбирается случайный сотрудник из перемешанного списка, отличный от
    * текущего исполнителя (`newAssigneeId != oldAssigneeId`). Если доступно менее двух сотрудников — задачи не
    * перераспределяются.
    */
  def shuffle(tasks: List[Task], popugs: List[UserId], random: Random): List[Reassignment] =
    if popugs.size < 2 then Nil
    else
      val order = random.shuffle(popugs)
      tasks.flatMap { task =>
        order.find(_ != task.assigneeId).map(popug => Reassignment(task.id, task.assigneeId, popug))
      }
