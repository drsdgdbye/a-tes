package inc.uberpopug.taskservice.api

import java.util.UUID

import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*

/** Декларативное описание HTTP-эндпоинтов Task Service через tapir. Личность берётся из `X-Auth-*` заголовков, которые
  * инжектит Gateway после верификации JWT.
  */
object TaskEndpoints:
  import TaskDtos.given

  /** Единый формат ошибки: HTTP-статус + JSON-тело `ErrorResponse`. */
  private val jsonErrorOut: EndpointOutput[(StatusCode, ErrorResponse)] =
    statusCode.and(jsonBody[ErrorResponse])

  /** Identity-заголовки, добавляемые Gateway: id и роль верифицированного пользователя. */
  private val identityIn: EndpointInput[(String, String)] =
    header[String]("X-Auth-User-Id").and(header[String]("X-Auth-User-Role"))

  /** `GET /health` — liveness-проверка. */
  val health: PublicEndpoint[Unit, Unit, HealthResponse, Any] =
    endpoint.get.in("health").out(jsonBody[HealthResponse])

  /** `GET /ready` — readiness-проверка. */
  val ready: PublicEndpoint[Unit, Unit, HealthResponse, Any] =
    endpoint.get.in("ready").out(jsonBody[HealthResponse])

  /** `POST /tasks` — создание задачи (все). */
  val createTask: PublicEndpoint[(String, String, CreateTaskRequest), (StatusCode, ErrorResponse), TaskResponse, Any] =
    endpoint.post
      .in(identityIn)
      .in("tasks")
      .in(jsonBody[CreateTaskRequest])
      .out(jsonBody[TaskResponse])
      .errorOut(jsonErrorOut)

  /** `GET /tasks` — мои задачи с пагинацией (все). */
  val listMyTasks: PublicEndpoint[
    (String, String, Option[Int], Option[Int]),
    (StatusCode, ErrorResponse),
    TasksResponse,
    Any
  ] =
    endpoint.get
      .in(identityIn)
      .in("tasks")
      .in(query[Option[Int]]("limit"))
      .in(query[Option[Int]]("offset"))
      .out(jsonBody[TasksResponse])
      .errorOut(jsonErrorOut)

  /** `GET /tasks/all` — все задачи с пагинацией (все). */
  val listAllTasks: PublicEndpoint[
    (String, String, Option[Int], Option[Int]),
    (StatusCode, ErrorResponse),
    TasksResponse,
    Any
  ] =
    endpoint.get
      .in(identityIn)
      .in("tasks" / "all")
      .in(query[Option[Int]]("limit"))
      .in(query[Option[Int]]("offset"))
      .out(jsonBody[TasksResponse])
      .errorOut(jsonErrorOut)

  /** `GET /tasks/{id}` — задача по id (все). */
  val getTask: PublicEndpoint[(String, String, UUID), (StatusCode, ErrorResponse), TaskResponse, Any] =
    endpoint.get
      .in(identityIn)
      .in("tasks" / path[UUID])
      .out(jsonBody[TaskResponse])
      .errorOut(jsonErrorOut)

  /** `PATCH /tasks/{id}/complete` — завершение задачи (исполнитель). */
  val completeTask: PublicEndpoint[(String, String, UUID), (StatusCode, ErrorResponse), TaskResponse, Any] =
    endpoint.patch
      .in(identityIn)
      .in("tasks" / path[UUID] / "complete")
      .out(jsonBody[TaskResponse])
      .errorOut(jsonErrorOut)

  /** `POST /tasks/shuffle` — перетасовка задач (admin/manager). */
  val shuffle: PublicEndpoint[(String, String), (StatusCode, ErrorResponse), ShuffleResponse, Any] =
    endpoint.post
      .in(identityIn)
      .in("tasks" / "shuffle")
      .out(jsonBody[ShuffleResponse])
      .errorOut(jsonErrorOut)

  /** Все эндпоинты сервиса для монтажа в HTTP-сервер. Конкретные пути (`all`, `shuffle`) идут до шаблонных (`{id}`),
    * чтобы не конфликтовать при маршрутизации.
    */
  val all: List[AnyEndpoint] =
    List(
      health,
      ready,
      createTask,
      listMyTasks,
      listAllTasks,
      getTask,
      shuffle,
      completeTask
    )
