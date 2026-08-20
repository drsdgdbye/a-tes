package inc.uberpopug.taskservice.repository

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import inc.uberpopug.common.domain.{DomainError, Money, TaskId, UserId}
import inc.uberpopug.common.domain.DomainError.{OptimisticLockConflict, PersistenceError}
import inc.uberpopug.taskservice.db.DbContext.Postgres
import inc.uberpopug.taskservice.domain.{Task, TaskDescription, TaskStatus, TaskTitle}
import io.getquill.*
import zio.{ZIO, ZLayer}

/** Строка таблицы `tasks` в формате БД. */
final case class TaskRow(
    id: UUID,
    title: String,
    description: Option[String],
    status: String,
    assigneeId: UUID,
    assignFeeCents: Long,
    completeRewardCents: Long,
    createdAt: Instant,
    completedAt: Option[Instant],
    version: Long
)

object TaskRow:
  /** Маппит доменную задачу в строку БД (enum → wire-строка, Money → центы). */
  def fromTask(task: Task): TaskRow =
    TaskRow(
      id = task.id.value,
      title = task.title.value,
      description = task.description.map(_.value),
      status = task.status.wire,
      assigneeId = task.assigneeId.value,
      assignFeeCents = task.assignFee.toCents,
      completeRewardCents = task.completeReward.toCents,
      createdAt = task.createdAt,
      completedAt = task.completedAt,
      version = task.version
    )

  /** Восстанавливает доменную задачу из строки БД; повреждённая строка — ошибка. */
  def toTask(row: TaskRow): Either[DomainError, Task] =
    for status <- TaskStatus.from(row.status)
    yield Task(
      id = TaskId(row.id),
      title = TaskTitle(row.title),
      description = row.description.map(TaskDescription(_)),
      status = status,
      assigneeId = UserId(row.assigneeId),
      assignFee = Money.fromCents(row.assignFeeCents),
      completeReward = Money.fromCents(row.completeRewardCents),
      createdAt = row.createdAt,
      completedAt = row.completedAt,
      version = row.version
    )

/** Репозиторий задач с атомарными операциями для race-сценариев (optimistic lock + outbox). */
trait TaskRepository:
  /** Создаёт задачу и добавляет события в outbox в одной транзакции. */
  def createWithOutbox(task: Task, outbox: List[OutboxRecord]): ZIO[Any, DomainError, Unit]

  /** Ищет задачу по id. */
  def findById(id: TaskId): ZIO[Any, DomainError, Option[Task]]

  /** Возвращает страницу задач заданного исполнителя (все статусы) для `GET /tasks` (мои). */
  def listByAssignee(assigneeId: UserId, limit: Int, offset: Int): ZIO[Any, DomainError, List[Task]]

  /** Считает задачи заданного исполнителя для `total`. */
  def countByAssignee(assigneeId: UserId): ZIO[Any, DomainError, Long]

  /** Возвращает страницу всех задач для `GET /tasks/all`. */
  def listAll(limit: Int, offset: Int): ZIO[Any, DomainError, List[Task]]

  /** Считает все задачи для `total`. */
  def countAll: ZIO[Any, DomainError, Long]

  /** Возвращает страницу открытых задач (для перетасовки). */
  def listOpen(limit: Int, offset: Int): ZIO[Any, DomainError, List[Task]]

  /** Переводит задачу в `completed` через optimistic lock: `WHERE version = expected AND status = 'open'`. Добавляет
    * событие в outbox в той же транзакции. Если задача не найдена/не открыта/изменена — `OptimisticLockConflict`.
    */
  def completeWithOutbox(completed: Task, expectedVersion: Long, outbox: OutboxRecord): ZIO[Any, DomainError, Unit]

  /** Меняет исполнителя через optimistic lock: `WHERE version = expected AND status = 'open'`. При применении добавляет
    * событие в outbox в той же транзакции; `false` при несовпадении версии/статуса.
    */
  def reassignWithOutbox(reassigned: Task, expectedVersion: Long, outbox: OutboxRecord): ZIO[Any, DomainError, Boolean]

object TaskRepository:
  /** Слой репозитория поверх Quill-контекста Postgres. */
  val layer: ZLayer[Postgres, Nothing, TaskRepository] =
    ZLayer.fromFunction(TaskRepositoryLive(_))

/** Quill-реализация репозитория задач поверх Postgres. */
final case class TaskRepositoryLive(ctx: Postgres) extends TaskRepository:
  import ctx.*
  import Tables.given

  /** Оборачивает SQL-ошибку в `PersistenceError`. */
  private def toPersistenceError(ex: Throwable): DomainError =
    ex match
      case e: SQLException => PersistenceError(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
      case other           => PersistenceError(Option(other.getMessage).getOrElse(other.getClass.getSimpleName))

  /** Парсит строку БД в доменную задачу; повреждённая строка — ошибка. */
  private def parseRow(row: TaskRow): ZIO[Any, DomainError, Task] =
    ZIO.fromEither(TaskRow.toTask(row)).mapError(_ => PersistenceError(s"Corrupted task row: ${row.id}"))

  /** Вставка задачи и всех событий в outbox в одной транзакции. */
  def createWithOutbox(task: Task, outbox: List[OutboxRecord]): ZIO[Any, DomainError, Unit] =
    ctx
      .transaction {
        run(query[TaskRow].insertValue(lift(TaskRow.fromTask(task)))) *>
          ZIO.foreachDiscard(outbox) { record =>
            run(query[OutboxRow].insertValue(lift(OutboxRow.fromRecord(record))))
          }
      }
      .unit
      .mapError(toPersistenceError)

  /** Ищет задачу по id; отсутствие — `None`. */
  def findById(id: TaskId): ZIO[Any, DomainError, Option[Task]] =
    run(query[TaskRow].filter(_.id == lift(id.value)))
      .map(_.headOption)
      .mapError(toPersistenceError)
      .flatMap {
        case Some(row) => parseRow(row).map(Some(_))
        case None      => ZIO.none
      }

  /** Страница задач заданного исполнителя, по возрастанию создания. */
  def listByAssignee(assigneeId: UserId, limit: Int, offset: Int): ZIO[Any, DomainError, List[Task]] =
    run(
      query[TaskRow]
        .filter(_.assigneeId == lift(assigneeId.value))
        .sortBy(_.createdAt)(using Ord.asc)
        .drop(lift(offset))
        .take(lift(limit))
    )
      .mapError(toPersistenceError)
      .flatMap(rows => ZIO.foreach(rows)(parseRow))

  /** Считает задачи заданного исполнителя. */
  def countByAssignee(assigneeId: UserId): ZIO[Any, DomainError, Long] =
    run(query[TaskRow].filter(_.assigneeId == lift(assigneeId.value)).size)
      .mapError(toPersistenceError)

  /** Страница всех задач, по возрастанию создания. */
  def listAll(limit: Int, offset: Int): ZIO[Any, DomainError, List[Task]] =
    run(
      query[TaskRow]
        .sortBy(_.createdAt)(using Ord.asc)
        .drop(lift(offset))
        .take(lift(limit))
    )
      .mapError(toPersistenceError)
      .flatMap(rows => ZIO.foreach(rows)(parseRow))

  /** Считает все задачи. */
  def countAll: ZIO[Any, DomainError, Long] =
    run(query[TaskRow].size)
      .mapError(toPersistenceError)

  /** Страница открытых задач, по возрастанию создания (для перетасовки). */
  def listOpen(limit: Int, offset: Int): ZIO[Any, DomainError, List[Task]] =
    run(
      query[TaskRow]
        .filter(_.status == lift(TaskStatus.Open.wire))
        .sortBy(_.createdAt)(using Ord.asc)
        .drop(lift(offset))
        .take(lift(limit))
    )
      .mapError(toPersistenceError)
      .flatMap(rows => ZIO.foreach(rows)(parseRow))

  /** Оптимистичное завершение задачи: только открытая задача с ожидаемой версией. Обновление и запись outbox — в одной
    * транзакции; несовпадение версии/статуса — `OptimisticLockConflict`.
    */
  def completeWithOutbox(completed: Task, expectedVersion: Long, outbox: OutboxRecord): ZIO[Any, DomainError, Unit] =
    for
      applied <- ctx
        .transaction {
          for
            affected <- run(
              query[TaskRow]
                .filter(_.id == lift(completed.id.value))
                .filter(_.version == lift(expectedVersion))
                .filter(_.status == lift(TaskStatus.Open.wire))
                .update(
                  _.status -> lift(TaskStatus.Completed.wire),
                  _.completedAt -> lift(completed.completedAt),
                  _.version -> lift(completed.version)
                )
            )
            ok <-
              if affected == 0 then ZIO.succeed(false)
              else run(query[OutboxRow].insertValue(lift(OutboxRow.fromRecord(outbox)))).as(true)
          yield ok
        }
        .mapError(toPersistenceError)
      _ <-
        if applied then ZIO.unit
        else ZIO.fail(OptimisticLockConflict(s"Task ${completed.id.value} changed concurrently"))
    yield ()

  /** Оптимистичный реассайн: `false` при несовпадении версии/статуса, иначе событие в outbox. */
  def reassignWithOutbox(
      reassigned: Task,
      expectedVersion: Long,
      outbox: OutboxRecord
  ): ZIO[Any, DomainError, Boolean] =
    ctx
      .transaction {
        for
          affected <- run(
            query[TaskRow]
              .filter(_.id == lift(reassigned.id.value))
              .filter(_.version == lift(expectedVersion))
              .filter(_.status == lift(TaskStatus.Open.wire))
              .update(
                _.assigneeId -> lift(reassigned.assigneeId.value),
                _.version -> lift(reassigned.version)
              )
          )
          applied <-
            if affected == 0 then ZIO.succeed(false)
            else run(query[OutboxRow].insertValue(lift(OutboxRow.fromRecord(outbox)))).as(true)
        yield applied
      }
      .mapError(toPersistenceError)
