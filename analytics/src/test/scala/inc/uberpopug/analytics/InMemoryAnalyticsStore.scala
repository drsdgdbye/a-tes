package inc.uberpopug.analytics

import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID

import zio.{Ref, ZIO}

import inc.uberpopug.analytics.domain.{CompletedTask, DailyStat, PopugInMinus, TaskStatus}
import inc.uberpopug.analytics.repository.{AnalyticsStore, DailyStatsRow, PopugBalanceRow, TaskRow}
import inc.uberpopug.common.domain.{DomainError, TaskId, UserId}

/** Упрощает `.orDie` для типизированных ошибок `DomainError` (не `Throwable`): превращает ошибку в исключение и роняет
  * эффект — «этого не должно случиться в тесте».
  */
extension [R, A](io: ZIO[R, DomainError, A])
  def orDieE: ZIO[R, Nothing, A] = io.mapError(error => new RuntimeException(error.toString)).orDie

/** In-memory состояние read-side проекций для тестов: задачи, балансы попугов, ежедневная статистика, дедупликация. */
final case class InMemoryAnalyticsState(
    tasks: Map[UUID, TaskRow] = Map.empty,
    popugs: Map[UUID, PopugBalanceRow] = Map.empty,
    dailyStats: Map[LocalDate, DailyStatsRow] = Map.empty,
    processed: Set[UUID] = Set.empty
)

/** In-memory реализация `AnalyticsStore` на Ref с той же семантикой, что и Quill-реализация: dedup по `eventId`,
  * атомарность, инкрементальные проекции.
  */
final case class InMemoryAnalyticsStore(state: Ref[InMemoryAnalyticsState]) extends AnalyticsStore:
  /** UTC-дата момента времени. */
  private def utcDate(at: Instant): LocalDate = at.atZone(ZoneOffset.UTC).toLocalDate

  def upsertUser(eventId: UUID, userId: UserId, name: String, at: Instant): ZIO[Any, DomainError, Boolean] =
    state.modify { s =>
      if s.processed.contains(eventId) then (false, s)
      else
        val popugs = s.popugs + (userId.value -> PopugBalanceRow(userId.value, name, 0L, at))
        val stats = bumpPopugsTotal(s, utcDate(at), 1, at)
        (true, s.copy(popugs = popugs, dailyStats = stats, processed = s.processed + eventId))
    }

  def insertTask(
      eventId: UUID,
      taskId: TaskId,
      title: String,
      assignFeeCents: Long,
      completeRewardCents: Long,
      createdAt: Instant
  ): ZIO[Any, DomainError, Boolean] =
    state.modify { s =>
      if s.processed.contains(eventId) then (false, s)
      else
        val tasks = s.tasks + (taskId.value -> TaskRow(
          taskId.value,
          title,
          assignFeeCents,
          completeRewardCents,
          TaskStatus.Open.wire,
          createdAt,
          None
        ))
        val stats = bumpEarnings(s, utcDate(createdAt), assignFeeCents - completeRewardCents, createdAt)
        (true, s.copy(tasks = tasks, dailyStats = stats, processed = s.processed + eventId))
    }

  def completeTask(
      eventId: UUID,
      taskId: TaskId,
      assigneeId: UserId,
      completeRewardCents: Long,
      completedAt: Instant
  ): ZIO[Any, DomainError, Boolean] =
    state.modify { s =>
      if s.processed.contains(eventId) then (false, s)
      else
        val tasks = s.tasks.get(taskId.value) match
          case Some(row) =>
            s.tasks.updated(taskId.value, row.copy(status = TaskStatus.Completed.wire, completedAt = Some(completedAt)))
          case None => s.tasks
        val popugs = adjustBalance(s.popugs, assigneeId.value, completeRewardCents, completedAt)
        (true, s.copy(tasks = tasks, popugs = popugs, processed = s.processed + eventId))
    }

  def applyAssignment(
      eventId: UUID,
      newAssignee: UserId,
      oldAssignee: Option[UserId],
      assignFeeCents: Long,
      at: Instant
  ): ZIO[Any, DomainError, Boolean] =
    state.modify { s =>
      if s.processed.contains(eventId) then (false, s)
      else
        val afterNew = adjustBalance(s.popugs, newAssignee.value, -assignFeeCents, at)
        val popugs = oldAssignee.fold(afterNew)(id => adjustBalance(afterNew, id.value, assignFeeCents, at))
        (true, s.copy(popugs = popugs, processed = s.processed + eventId))
    }

  def applyPayout(
      eventId: UUID,
      popugId: UserId,
      popugName: String,
      amountCents: Long,
      date: LocalDate,
      at: Instant
  ): ZIO[Any, DomainError, Boolean] =
    state.modify { s =>
      if s.processed.contains(eventId) then (false, s)
      else
        val afterAdjust = adjustBalance(s.popugs, popugId.value, -amountCents, at)
        val afterPayout = afterAdjust.updated(
          popugId.value,
          afterAdjust(popugId.value).copy(name = popugName, updatedAt = at)
        )
        val negative = afterPayout.values.count(_.balanceCents < 0)
        val stats = setNegative(s, date, negative, at)
        (true, s.copy(popugs = afterPayout, dailyStats = stats, processed = s.processed + eventId))
    }

  def findPopug(userId: UserId): ZIO[Any, DomainError, Option[String]] =
    state.get.map(_.popugs.get(userId.value).map(_.name))

  def managementEarnings(from: LocalDate, to: LocalDate): ZIO[Any, DomainError, List[DailyStat]] =
    state.get.map { s =>
      s.dailyStats.toList
        .filter { case (date, _) => !date.isBefore(from) && !date.isAfter(to) }
        .sortBy(_._1)
        .map { case (date, row) => DailyStat(date, row.topManagementEarningsCents) }
    }

  def popugsInMinus: ZIO[Any, DomainError, List[PopugInMinus]] =
    state.get.map { s =>
      s.popugs.values
        .filter(_.balanceCents < 0)
        .toList
        .sortBy(_.name)
        .map(row => PopugInMinus(row.userId, row.name, row.balanceCents))
    }

  def mostExpensiveTasks(from: Instant, to: Instant): ZIO[Any, DomainError, List[CompletedTask]] =
    state.get.map { s =>
      s.tasks.values
        .filter(row => row.status == TaskStatus.Completed.wire)
        .flatMap(row => row.completedAt.map(at => (row, at)))
        .filter { case (_, at) => !at.isBefore(from) && at.isBefore(to) }
        .map { case (row, at) => CompletedTask(row.taskId, row.title, row.completeRewardCents, utcDate(at)) }
        .toList
    }

  private def bumpEarnings(
      s: InMemoryAnalyticsState,
      date: LocalDate,
      delta: Long,
      at: Instant
  ): Map[LocalDate, DailyStatsRow] =
    val row = s.dailyStats.getOrElse(date, DailyStatsRow(date, 0L, 0, 0, at))
    s.dailyStats + (date -> row.copy(
      topManagementEarningsCents = row.topManagementEarningsCents + delta,
      updatedAt = at
    ))

  private def bumpPopugsTotal(
      s: InMemoryAnalyticsState,
      date: LocalDate,
      delta: Int,
      at: Instant
  ): Map[LocalDate, DailyStatsRow] =
    val row = s.dailyStats.getOrElse(date, DailyStatsRow(date, 0L, 0, 0, at))
    s.dailyStats + (date -> row.copy(popugsTotal = row.popugsTotal + delta, updatedAt = at))

  private def setNegative(
      s: InMemoryAnalyticsState,
      date: LocalDate,
      count: Int,
      at: Instant
  ): Map[LocalDate, DailyStatsRow] =
    val row = s.dailyStats.getOrElse(date, DailyStatsRow(date, 0L, 0, 0, at))
    s.dailyStats + (date -> row.copy(popugsNegative = count, updatedAt = at))

  private def adjustBalance(
      popugs: Map[UUID, PopugBalanceRow],
      userId: UUID,
      delta: Long,
      at: Instant
  ): Map[UUID, PopugBalanceRow] =
    popugs.get(userId) match
      case Some(row) => popugs + (userId -> row.copy(balanceCents = row.balanceCents + delta, updatedAt = at))
      case None      => popugs
