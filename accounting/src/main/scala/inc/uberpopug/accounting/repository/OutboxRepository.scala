package inc.uberpopug.accounting.repository

import java.time.Instant
import java.util.UUID

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.PersistenceError
import inc.uberpopug.accounting.db.DbContext.Postgres
import io.getquill.*
import zio.{ZIO, ZLayer}

/** Событие для transactional outbox до сериализации в строку БД. */
final case class OutboxRecord(
    aggregateId: UUID,
    eventType: String,
    payload: Array[Byte],
    createdAt: Instant
)

/** Строка таблицы `outbox` в формате БД. */
final case class OutboxRow(
    id: Long,
    aggregateId: UUID,
    eventType: String,
    payload: Array[Byte],
    createdAt: Instant,
    published: Boolean
)

object OutboxRow:
  /** Превращает доменную запись в строку БД с флагом `published = false`. */
  def fromRecord(record: OutboxRecord): OutboxRow =
    OutboxRow(0L, record.aggregateId, record.eventType, record.payload, record.createdAt, published = false)

  /** Исключает `id` из INSERT: значение генерирует `BIGSERIAL` (иначе `id = 0` на второй записи ломается
    * `duplicate key` 23505).
    */
  inline given InsertMeta[OutboxRow] = insertMeta[OutboxRow](_.id)

/** Репозиторий transactional outbox: буфер событий для Kafka-публикации. Вставка записей выполняется атомарно вместе с
  * событием, породившим их (см. `EventStore.appendWithOutbox`); здесь — операции relay.
  */
trait OutboxRepository:
  /** Добавляет событие в outbox. */
  def insert(record: OutboxRecord): ZIO[Any, DomainError, Unit]

  /** Забирает батч ещё не опубликованных событий в порядке создания. */
  def claimBatch(limit: Int): ZIO[Any, DomainError, List[OutboxRow]]

  /** Помечает список событий опубликованными. */
  def markPublished(ids: List[Long]): ZIO[Any, DomainError, Unit]

object OutboxRepository:
  /** Слой репозитория поверх Quill-контекста Postgres. */
  val layer: ZLayer[Postgres, Nothing, OutboxRepository] =
    ZLayer.fromFunction(OutboxRepositoryLive(_))

/** Quill-реализация репозитория outbox поверх Postgres. */
final case class OutboxRepositoryLive(ctx: Postgres) extends OutboxRepository:
  import ctx.*

  /** Оборачивает SQL-ошибку в `PersistenceError`. */
  private def toPersistenceError(ex: Throwable): DomainError =
    PersistenceError(Option(ex.getMessage).getOrElse(ex.getClass.getSimpleName))

  /** Добавляет событие в outbox. */
  def insert(record: OutboxRecord): ZIO[Any, DomainError, Unit] =
    run(query[OutboxRow].insertValue(lift(OutboxRow.fromRecord(record)))).unit
      .mapError(toPersistenceError)

  /** Выбирает до `limit` неопубликованных событий по возрастанию id. */
  def claimBatch(limit: Int): ZIO[Any, DomainError, List[OutboxRow]] =
    run(
      query[OutboxRow]
        .filter(_.published == false)
        .sortBy(_.id)(using Ord.asc)
        .take(lift(limit))
    ).mapError(toPersistenceError)

  /** Помечает события опубликованными (пустой список — no-op). */
  def markPublished(ids: List[Long]): ZIO[Any, DomainError, Unit] =
    if ids.isEmpty then ZIO.unit
    else
      run(
        query[OutboxRow]
          .filter(row => liftQuery(ids).contains(row.id))
          .update(_.published -> true)
      ).unit
        .mapError(toPersistenceError)
