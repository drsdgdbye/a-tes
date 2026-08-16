package inc.uberpopug.taskservice.service

import java.util.UUID

import scala.util.Random

import zio.{Clock, ZIO, ZLayer}

import inc.uberpopug.common.domain.{DomainError, TaskId}
import inc.uberpopug.common.domain.DomainError.*
import inc.uberpopug.taskservice.domain.*
import inc.uberpopug.taskservice.repository.TaskRepository

/** Use case'ы Task Service. Один метод — одна операция. */
trait TaskService:
  /** Создаёт задачу: валидация, случайный исполнитель, цены из PricingPolicy, атомарно task + outbox (TaskCreated +
    * TaskAssigned).
    */
  def createTask(title: String, description: Option[String], actor: AuthenticatedUser): ZIO[Clock, DomainError, Task]

  /** Мои задачи (исполнитель = actor) с пагинацией. */
  def listMyTasks(limit: Int, offset: Int, actor: AuthenticatedUser): ZIO[Any, DomainError, (List[Task], Long)]

  /** Все задачи с пагинацией. */
  def listAllTasks(limit: Int, offset: Int, actor: AuthenticatedUser): ZIO[Any, DomainError, (List[Task], Long)]

  /** Задача по id. */
  def getTask(id: TaskId, actor: AuthenticatedUser): ZIO[Any, DomainError, Task]

  /** Завершение задачи исполнителем: только `open → completed`, optimistic lock, outbox TaskCompleted. */
  def completeTask(id: TaskId, actor: AuthenticatedUser): ZIO[Clock, DomainError, Task]

  /** Перетасовка открытых задач между попугами (admin/manager): реассайн + TaskAssigned, возвращает число задач. */
  def shuffle(actor: AuthenticatedUser): ZIO[Clock, DomainError, Int]

object TaskService:
  /** Слой сервиса поверх репозитория задач, кэша попугов и генератора случайных чисел. Clock подставляется на месте
    * вызова.
    */
  val layer: ZLayer[TaskRepository & EligiblePopugs, Nothing, TaskService] =
    ZLayer.fromFunction(TaskServiceLive(_, _, Random()))

/** Реализация TaskService: оркестрация репозиториев, кэша попугов и доменных политик. */
final case class TaskServiceLive(
    tasks: TaskRepository,
    eligible: EligiblePopugs,
    random: Random
) extends TaskService:

  /** Размер страницы при чтении открытых задач для перетасовки. */
  private val ShuffleBatchSize = 100

  /** Создание задачи: validation через smart-конструкторы, пустое описание → `None`, случайный исполнитель и цены,
    * атомарная запись task + outbox.
    */
  def createTask(title: String, description: Option[String], actor: AuthenticatedUser): ZIO[Clock, DomainError, Task] =
    for
      taskTitle <- ZIO.fromEither(TaskTitle.from(title))
      desc <- ZIO.foreach(description.filter(_.trim.nonEmpty))(d => ZIO.fromEither(TaskDescription.from(d)))
      popugs <- eligible.all
      assignee <- ZIO.fromEither(AssignmentPolicy.assignRandom(popugs.toList, random))
      now <- Clock.instant
      task <- ZIO.fromEither(
        Task.create(
          id = TaskId(UUID.randomUUID()),
          title = taskTitle,
          description = desc,
          assigneeId = assignee,
          assignFee = PricingPolicy.generateAssignFee(random),
          completeReward = PricingPolicy.generateCompleteReward(random),
          now = now
        )
      )
      _ <- tasks.createWithOutbox(
        task,
        List(TaskEventBuilders.taskCreated(task, now), TaskEventBuilders.taskAssigned(task, None, now))
      )
    yield task

  /** Мои задачи: список + total, сортировка по созданию. */
  def listMyTasks(limit: Int, offset: Int, actor: AuthenticatedUser): ZIO[Any, DomainError, (List[Task], Long)] =
    for
      items <- tasks.listByAssignee(actor.id, limit, offset)
      total <- tasks.countByAssignee(actor.id)
    yield (items, total)

  /** Все задачи: список + total. */
  def listAllTasks(limit: Int, offset: Int, actor: AuthenticatedUser): ZIO[Any, DomainError, (List[Task], Long)] =
    for
      items <- tasks.listAll(limit, offset)
      total <- tasks.countAll
    yield (items, total)

  /** Задача по id; отсутствие — `TaskNotFound`. */
  def getTask(id: TaskId, actor: AuthenticatedUser): ZIO[Any, DomainError, Task] =
    tasks.findById(id).flatMap {
      case Some(task) => ZIO.succeed(task)
      case None       => ZIO.fail(TaskNotFound(id.value.toString))
    }

  /** Завершение: только исполнитель, только открытая задача, optimistic lock, outbox TaskCompleted. */
  def completeTask(id: TaskId, actor: AuthenticatedUser): ZIO[Clock, DomainError, Task] =
    for
      now <- Clock.instant
      task <- tasks.findById(id).flatMap {
        case Some(task) => ZIO.succeed(task)
        case None       => ZIO.fail(TaskNotFound(id.value.toString))
      }
      _ <-
        if task.assigneeId == actor.id then ZIO.unit
        else ZIO.fail(AccessDenied("Only the assignee can complete the task"))
      completed <- ZIO.fromEither(Task.complete(task, now))
      _ <- tasks.completeWithOutbox(completed, task.version, TaskEventBuilders.taskCompleted(completed, now))
    yield completed

  /** Перетасовка: только admin/manager; задачи, закрытые в момент обновления, пропускаются через optimistic lock.
    * Возвращает число успешно перераспределённых задач.
    */
  def shuffle(actor: AuthenticatedUser): ZIO[Clock, DomainError, Int] =
    for
      _ <- requireManagerOrAdmin(actor)
      now <- Clock.instant
      popugs <- eligible.all
      openTasks <- allOpenTasks
      reassignments = AssignmentPolicy.shuffle(openTasks, popugs.toList, random)
      counts <- ZIO.foreach(reassignments) { reassignment =>
        for
          applied <- applyReassignment(reassignment, now)
          _ <-
            if applied then ZIO.logInfo(s"Reassigned task ${reassignment.taskId.value}")
            else ZIO.logInfo(s"Task ${reassignment.taskId.value} skipped during shuffle (concurrent change)")
        yield applied
      }
    yield counts.count(identity)

  /** Применяет реассайн одной задачи через optimistic lock; `false` — задача закрыта/изменена, outbox не пишется. */
  private def applyReassignment(
      reassignment: Reassignment,
      now: java.time.Instant
  ): ZIO[Any, DomainError, Boolean] =
    tasks.findById(reassignment.taskId).flatMap {
      case None       => ZIO.succeed(false)
      case Some(task) =>
        val reassigned = Task.reassign(task, reassignment.newAssigneeId)
        tasks.reassignWithOutbox(
          reassigned,
          task.version,
          TaskEventBuilders.taskAssigned(reassigned, Some(task.assigneeId), now)
        )
    }

  /** Читает все открытые задачи страницами (до пустой страницы). */
  private def allOpenTasks: ZIO[Any, DomainError, List[Task]] =
    def loop(offset: Int, acc: List[Task]): ZIO[Any, DomainError, List[Task]] =
      tasks.listOpen(ShuffleBatchSize, offset).flatMap { page =>
        if page.isEmpty then ZIO.succeed(acc)
        else loop(offset + page.size, acc ++ page)
      }
    loop(0, Nil)

  /** Проверяет права на перетасовку: admin или manager. */
  private def requireManagerOrAdmin(actor: AuthenticatedUser): ZIO[Any, DomainError, Unit] =
    actor.role match
      case Role.Admin | Role.Manager => ZIO.unit
      case _                         => ZIO.fail(AccessDenied("Admin or manager privileges required"))
