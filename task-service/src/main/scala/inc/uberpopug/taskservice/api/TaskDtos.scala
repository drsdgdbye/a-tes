package inc.uberpopug.taskservice.api

import java.time.Instant

import zio.json.{DeriveJsonCodec, JsonCodec}

/** Тело запроса `POST /tasks`: заголовок и необязательное описание. */
final case class CreateTaskRequest(title: String, description: Option[String])

/** Представление задачи в API: деньги — строки вида `"15.00"`. */
final case class TaskResponse(
    id: String,
    title: String,
    description: Option[String],
    status: String,
    assigneeId: String,
    assignFee: String,
    completeReward: String,
    createdAt: Instant,
    completedAt: Option[Instant]
)

/** Ответ `GET /tasks` и `GET /tasks/all` — страница списка. */
final case class TasksResponse(items: List[TaskResponse], total: Long)

/** Ответ `POST /tasks/shuffle` — число перераспределённых задач. */
final case class ShuffleResponse(tasksReassigned: Int)

/** Ответ liveness/readiness эндпоинтов. */
final case class HealthResponse(status: String)

object TaskDtos:
  given JsonCodec[ErrorResponse] = DeriveJsonCodec.gen
  given JsonCodec[CreateTaskRequest] = DeriveJsonCodec.gen
  given JsonCodec[TaskResponse] = DeriveJsonCodec.gen
  given JsonCodec[TasksResponse] = DeriveJsonCodec.gen
  given JsonCodec[ShuffleResponse] = DeriveJsonCodec.gen
  given JsonCodec[HealthResponse] = DeriveJsonCodec.gen
