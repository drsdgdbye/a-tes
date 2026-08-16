package inc.uberpopug.taskservice.api

import sttp.model.StatusCode
import sttp.tapir.ztapir.*
import zio.{Clock, ZIO, ZLayer}

import inc.uberpopug.common.domain.{Pagination, TaskId, UserId}
import inc.uberpopug.taskservice.domain.{Role, Task}
import inc.uberpopug.taskservice.service.{AuthenticatedUser, TaskService}

/** Server logic: связывает tapir-эндпоинты с TaskService и маппингом ошибок. Личность достаётся из `X-Auth-*`
  * заголовков Gateway.
  */
final case class TaskServerLogic(taskService: TaskService):
  import TaskEndpoints.*
  import TaskServerLogic.*

  /** Восстанавливает верифицированную личность из identity-заголовков. */
  private def identity(userId: String, role: String): ZIO[Any, (StatusCode, ErrorResponse), AuthenticatedUser] =
    for
      uid <- ZIO.fromEither(UserId.from(userId)).mapError(ErrorMapping.toApiError)
      r <- ZIO.fromEither(Role.from(role)).mapError(ErrorMapping.toApiError)
    yield AuthenticatedUser(uid, r)

  /** Все эндпоинты сервиса: публичные health/ready и защищённые через identity-заголовки. */
  val endpoints: List[ZServerEndpoint[Clock, Any]] =
    List(
      health.zServerLogic[Clock](_ => ZIO.succeed(HealthResponse("ok"))),
      ready.zServerLogic[Clock](_ => ZIO.succeed(HealthResponse("ok"))),
      createTask.zServerLogic[Clock] { case (userId, role, req) =>
        for
          actor <- identity(userId, role)
          task <- taskService.createTask(req.title, req.description, actor).mapError(ErrorMapping.toApiError)
        yield toTaskResponse(task)
      },
      listMyTasks.zServerLogic[Clock] { case (userId, role, limit, offset) =>
        for
          actor <- identity(userId, role)
          pagination = Pagination.from(limit, offset)
          (items, total) <-
            taskService.listMyTasks(pagination.limit, pagination.offset, actor).mapError(ErrorMapping.toApiError)
        yield TasksResponse(items.map(toTaskResponse), total)
      },
      listAllTasks.zServerLogic[Clock] { case (userId, role, limit, offset) =>
        for
          actor <- identity(userId, role)
          pagination = Pagination.from(limit, offset)
          (items, total) <-
            taskService.listAllTasks(pagination.limit, pagination.offset, actor).mapError(ErrorMapping.toApiError)
        yield TasksResponse(items.map(toTaskResponse), total)
      },
      shuffle.zServerLogic[Clock] { case (userId, role) =>
        for
          actor <- identity(userId, role)
          count <- taskService.shuffle(actor).mapError(ErrorMapping.toApiError)
        yield ShuffleResponse(count)
      },
      getTask.zServerLogic[Clock] { case (userId, role, id) =>
        for
          actor <- identity(userId, role)
          taskId <- ZIO.fromEither(TaskId.from(id.toString)).mapError(ErrorMapping.toApiError)
          task <- taskService.getTask(taskId, actor).mapError(ErrorMapping.toApiError)
        yield toTaskResponse(task)
      },
      completeTask.zServerLogic[Clock] { case (userId, role, id) =>
        for
          actor <- identity(userId, role)
          taskId <- ZIO.fromEither(TaskId.from(id.toString)).mapError(ErrorMapping.toApiError)
          task <- taskService.completeTask(taskId, actor).mapError(ErrorMapping.toApiError)
        yield toTaskResponse(task)
      }
    )

object TaskServerLogic:
  /** Маппит доменную задачу в DTO ответа (деньги — строки с двумя знаками). */
  def toTaskResponse(task: Task): TaskResponse =
    TaskResponse(
      id = task.id.value.toString,
      title = task.title.value,
      description = task.description.map(_.value),
      status = task.status.wire,
      assigneeId = task.assigneeId.value.toString,
      assignFee = task.assignFee.value.toString,
      completeReward = task.completeReward.value.toString,
      createdAt = task.createdAt,
      completedAt = task.completedAt
    )

  /** Слой server logic поверх TaskService. */
  val layer: ZLayer[TaskService, Nothing, TaskServerLogic] =
    ZLayer.fromFunction(TaskServerLogic(_))
