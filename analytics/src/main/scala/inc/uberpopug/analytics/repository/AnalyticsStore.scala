package inc.uberpopug.analytics.repository

import java.sql.SQLException
import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID

import inc.uberpopug.common.domain.{DomainError, TaskId, UserId}
import inc.uberpopug.common.domain.DomainError.PersistenceError
import inc.uberpopug.analytics.db.DbContext.Postgres
import inc.uberpopug.analytics.domain.{CompletedTask, DailyStat, PopugInMinus, TaskStatus}
import io.getquill.*
import io.getquill.extras.*
import zio.{ZIO, ZLayer}

/** Строка таблицы `tasks` в формате БД. */
final case class TaskRow(
    taskId: UUID,
    title: String,
    assignFeeCents: Long,
    completeRewardCents: Long,
    status: String,
    createdAt: Instant,
    completedAt: Option[Instant]
)

/** Строка таблицы `popug_balances` в формате БД. */
final case class PopugBalanceRow(userId: UUID, name: String, balanceCents: Long, updatedAt: Instant)

/** Строка таблицы `daily_stats` в формате БД. */
final case class DailyStatsRow(
    date: LocalDate,
    topManagementEarningsCents: Long,
    popugsTotal: Int,
    popugsNegative: Int,
    updatedAt: Instant
)

/** Строка таблицы `processed_events` в формате БД: идемпотентность обработки событий. */
final case class ProcessedEventRow(eventId: UUID, eventType: String, processedAt: Instant)

/** Read-side проекции Analytics Service. Все записи — атомарные транзакции (processed_events dedup + update проекций +
  * daily_stats), поэтому повторная обработка одного Kafka-события идемпотентна. События для ещё не созданного попуга
  * отсутствуют в проверке — их обрабатывает `AnalyticsEventProcessor` (transient).
  */
trait AnalyticsStore:
  /** `UserCreated`: проекция попуга с нулевым балансом + `daily_stats.popugs_total`. `false` — уже обработано. */
  def upsertUser(eventId: UUID, userId: UserId, name: String, at: Instant): ZIO[Any, DomainError, Boolean]

  /** `TaskCreated`: задача в статусе `open` + `daily_stats.earnings` за дату создания. `false` — уже обработано. */
  def insertTask(
      eventId: UUID,
      taskId: TaskId,
      title: String,
      assignFeeCents: Long,
      completeRewardCents: Long,
      createdAt: Instant
  ): ZIO[Any, DomainError, Boolean]

  /** `TaskCompleted`: `tasks.status='completed'`, `completed_at` + начисление `completeReward` исполнителю. `false` —
    * уже обработано.
    */
  def completeTask(
      eventId: UUID,
      taskId: TaskId,
      assigneeId: UserId,
      completeRewardCents: Long,
      completedAt: Instant
  ): ZIO[Any, DomainError, Boolean]

  /** `TaskAssigned`: списание с нового исполнителя и возврат старому (если был). `false` — уже обработано. */
  def applyAssignment(
      eventId: UUID,
      newAssignee: UserId,
      oldAssignee: Option[UserId],
      assignFeeCents: Long,
      at: Instant
  ): ZIO[Any, DomainError, Boolean]

  /** `PaymentProcessed`: баланс попуга `-= amount`, имя из события, `daily_stats.popugs_negative` за дату выплаты.
    * `false` — уже обработано.
    */
  def applyPayout(
      eventId: UUID,
      popugId: UserId,
      popugName: String,
      amountCents: Long,
      date: LocalDate,
      at: Instant
  ): ZIO[Any, DomainError, Boolean]

  /** Имя попуга из проекции; отсутствие строки — `None`. */
  def findPopug(userId: UserId): ZIO[Any, DomainError, Option[String]]

  /** Доход менеджмента по дням за диапазон дат (включительно) из `daily_stats`. */
  def managementEarnings(from: LocalDate, to: LocalDate): ZIO[Any, DomainError, List[DailyStat]]

  /** Попуги с отрицательным балансом (текущее состояние проекции). */
  def popugsInMinus: ZIO[Any, DomainError, List[PopugInMinus]]

  /** Закрытые задачи с `completed_at` в полуинтервале `[from, to)`. */
  def mostExpensiveTasks(from: Instant, to: Instant): ZIO[Any, DomainError, List[CompletedTask]]

object AnalyticsStore:
  /** Слой репозитория поверх Quill-контекста Postgres. */
  val layer: ZLayer[Postgres, Nothing, AnalyticsStore] =
    ZLayer.fromFunction(AnalyticsStoreLive(_))

/** Quill-реализация read-side проекций поверх Postgres. */
final case class AnalyticsStoreLive(ctx: Postgres) extends AnalyticsStore:
  import ctx.*

  /** Оборачивает SQL-ошибку в `PersistenceError`. */
  private def toPersistenceError(ex: Throwable): DomainError =
    ex match
      case e: SQLException => PersistenceError(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
      case other           => PersistenceError(Option(other.getMessage).getOrElse(other.getClass.getSimpleName))

  /** UTC-дата момента времени: для привязки статистики к дню. */
  private def utcDate(at: Instant): LocalDate = at.atZone(ZoneOffset.UTC).toLocalDate

  /** Вставляет id события в `processed_events`; дубликат (SQLState 23505) — `false`. */
  private def insertProcessedIfAbsent(eventId: UUID, eventType: String, at: Instant): ZIO[Any, Throwable, Boolean] =
    run(query[ProcessedEventRow].insertValue(lift(ProcessedEventRow(eventId, eventType, at))))
      .as(true)
      .catchSome { case e: SQLException if e.getSQLState == "23505" => ZIO.succeed(false) }

  /** Инкрементальный upsert `daily_stats.earnings` за дату: прибавляет `delta` к существующей строке. */
  private def upsertEarnings(date: LocalDate, delta: Long, at: Instant): ZIO[Any, Throwable, Unit] =
    run(
      query[DailyStatsRow]
        .insertValue(lift(DailyStatsRow(date, delta, 0, 0, at)))
        .onConflictUpdate(_.date)(
          (t, e) => t.topManagementEarningsCents -> (t.topManagementEarningsCents + e.topManagementEarningsCents),
          (t, e) => t.updatedAt -> e.updatedAt
        )
    ).unit

  /** Инкрементальный upsert `daily_stats.popugs_total` за дату: прибавляет `delta` к существующей строке. */
  private def upsertPopugsTotal(date: LocalDate, delta: Int, at: Instant): ZIO[Any, Throwable, Unit] =
    run(
      query[DailyStatsRow]
        .insertValue(lift(DailyStatsRow(date, 0, delta, 0, at)))
        .onConflictUpdate(_.date)(
          (t, e) => t.popugsTotal -> (t.popugsTotal + e.popugsTotal),
          (t, e) => t.updatedAt -> e.updatedAt
        )
    ).unit

  /** Устанавливает `daily_stats.popugs_negative` за дату (снимок на момент обработки выплаты). */
  private def setPopugsNegative(date: LocalDate, count: Int, at: Instant): ZIO[Any, Throwable, Unit] =
    run(
      query[DailyStatsRow]
        .insertValue(lift(DailyStatsRow(date, 0, 0, count, at)))
        .onConflictUpdate(_.date)(
          (t, e) => t.popugsNegative -> e.popugsNegative,
          (t, e) => t.updatedAt -> e.updatedAt
        )
    ).unit

  def upsertUser(eventId: UUID, userId: UserId, name: String, at: Instant): ZIO[Any, DomainError, Boolean] =
    ctx
      .transaction {
        for
          deduplicated <- insertProcessedIfAbsent(eventId, "UserCreated", at)
          applied <-
            if !deduplicated then ZIO.succeed(false)
            else
              run(
                query[PopugBalanceRow]
                  .insertValue(lift(PopugBalanceRow(userId.value, name, 0L, at)))
                  .onConflictIgnore
              ).unit *>
                upsertPopugsTotal(utcDate(at), 1, at).as(true)
        yield applied
      }
      .mapError(toPersistenceError)

  def insertTask(
      eventId: UUID,
      taskId: TaskId,
      title: String,
      assignFeeCents: Long,
      completeRewardCents: Long,
      createdAt: Instant
  ): ZIO[Any, DomainError, Boolean] =
    ctx
      .transaction {
        for
          deduplicated <- insertProcessedIfAbsent(eventId, "TaskCreated", createdAt)
          applied <-
            if !deduplicated then ZIO.succeed(false)
            else
              run(
                query[TaskRow]
                  .insertValue(
                    lift(
                      TaskRow(
                        taskId.value,
                        title,
                        assignFeeCents,
                        completeRewardCents,
                        TaskStatus.Open.wire,
                        createdAt,
                        None
                      )
                    )
                  )
                  .onConflictIgnore
              ).unit *>
                upsertEarnings(utcDate(createdAt), assignFeeCents - completeRewardCents, createdAt).as(true)
        yield applied
      }
      .mapError(toPersistenceError)

  def completeTask(
      eventId: UUID,
      taskId: TaskId,
      assigneeId: UserId,
      completeRewardCents: Long,
      completedAt: Instant
  ): ZIO[Any, DomainError, Boolean] =
    ctx
      .transaction {
        for
          deduplicated <- insertProcessedIfAbsent(eventId, "TaskCompleted", completedAt)
          applied <-
            if !deduplicated then ZIO.succeed(false)
            else
              run(
                query[TaskRow]
                  .filter(_.taskId == lift(taskId.value))
                  .update(
                    r => r.status -> lift(TaskStatus.Completed.wire),
                    r => r.completedAt -> lift(Option(completedAt))
                  )
              ).unit *>
                run(
                  query[PopugBalanceRow]
                    .filter(_.userId == lift(assigneeId.value))
                    .update(
                      r => r.balanceCents -> (r.balanceCents + lift(completeRewardCents)),
                      r => r.updatedAt -> lift(completedAt)
                    )
                ).as(true)
        yield applied
      }
      .mapError(toPersistenceError)

  def applyAssignment(
      eventId: UUID,
      newAssignee: UserId,
      oldAssignee: Option[UserId],
      assignFeeCents: Long,
      at: Instant
  ): ZIO[Any, DomainError, Boolean] =
    ctx
      .transaction {
        for
          deduplicated <- insertProcessedIfAbsent(eventId, "TaskAssigned", at)
          applied <-
            if !deduplicated then ZIO.succeed(false)
            else
              run(
                query[PopugBalanceRow]
                  .filter(_.userId == lift(newAssignee.value))
                  .update(
                    r => r.balanceCents -> (r.balanceCents - lift(assignFeeCents)),
                    r => r.updatedAt -> lift(at)
                  )
              ).unit *>
                ZIO
                  .foreachDiscard(oldAssignee) { id =>
                    run(
                      query[PopugBalanceRow]
                        .filter(_.userId == lift(id.value))
                        .update(
                          r => r.balanceCents -> (r.balanceCents + lift(assignFeeCents)),
                          r => r.updatedAt -> lift(at)
                        )
                    ).unit
                  }
                  .as(true)
        yield applied
      }
      .mapError(toPersistenceError)

  def applyPayout(
      eventId: UUID,
      popugId: UserId,
      popugName: String,
      amountCents: Long,
      date: LocalDate,
      at: Instant
  ): ZIO[Any, DomainError, Boolean] =
    ctx
      .transaction {
        for
          deduplicated <- insertProcessedIfAbsent(eventId, "PaymentProcessed", at)
          applied <-
            if !deduplicated then ZIO.succeed(false)
            else
              for
                _ <- run(
                  query[PopugBalanceRow]
                    .filter(_.userId == lift(popugId.value))
                    .update(
                      r => r.balanceCents -> (r.balanceCents - lift(amountCents)),
                      r => r.name -> lift(popugName),
                      r => r.updatedAt -> lift(at)
                    )
                )
                negative <- run(query[PopugBalanceRow].filter(_.balanceCents < 0).size)
                _ <- setPopugsNegative(date, negative.toInt, at)
              yield true
        yield applied
      }
      .mapError(toPersistenceError)

  def findPopug(userId: UserId): ZIO[Any, DomainError, Option[String]] =
    run(query[PopugBalanceRow].filter(_.userId == lift(userId.value)))
      .map(_.headOption.map(_.name))
      .mapError(toPersistenceError)

  def managementEarnings(from: LocalDate, to: LocalDate): ZIO[Any, DomainError, List[DailyStat]] =
    run(
      query[DailyStatsRow]
        .filter(row => row.date >= lift(from) && row.date <= lift(to))
        .sortBy(_.date)(using Ord.asc)
    )
      .map(_.map(row => DailyStat(row.date, row.topManagementEarningsCents)))
      .mapError(toPersistenceError)

  def popugsInMinus: ZIO[Any, DomainError, List[PopugInMinus]] =
    run(
      query[PopugBalanceRow]
        .filter(_.balanceCents < 0)
        .sortBy(_.name)(using Ord.asc)
    )
      .map(_.map(row => PopugInMinus(row.userId, row.name, row.balanceCents)))
      .mapError(toPersistenceError)

  def mostExpensiveTasks(from: Instant, to: Instant): ZIO[Any, DomainError, List[CompletedTask]] =
    run(
      query[TaskRow]
        .filter(row =>
          row.status == lift(TaskStatus.Completed.wire) && row.completedAt.exists(c => c >= lift(from) && c < lift(to))
        )
    )
      .map { rows =>
        rows.flatMap { row =>
          row.completedAt.map(at => CompletedTask(row.taskId, row.title, row.completeRewardCents, utcDate(at)))
        }
      }
      .mapError(toPersistenceError)
