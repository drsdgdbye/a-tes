package inc.uberpopug.analytics.service

import java.time.{Instant, LocalDate, ZoneOffset}

import zio.{ZIO, ZLayer}

import inc.uberpopug.analytics.domain.{AnalyticsPeriod, CompletedTask, PopugInMinus, Role}
import inc.uberpopug.analytics.repository.AnalyticsStore
import inc.uberpopug.common.domain.{DomainError, Money}
import inc.uberpopug.common.domain.DomainError.{AccessDenied, InvalidValue}

/** Доход менеджмента за один день. */
final case class DailyEarnings(date: LocalDate, amount: Money)

/** Ответ `GET /analytics/top-management-earnings`: по дням за диапазон + итог. */
final case class ManagementEarningsReport(items: List[DailyEarnings], total: Money)

/** Ответ `GET /analytics/popugs-in-minus`: попуги с отрицательным балансом. */
final case class PopugsInMinusReport(count: Int, items: List[PopugInMinus])

/** Лучшая закрытая задача периода. */
final case class TopTask(taskId: java.util.UUID, title: String, amount: Money)

/** Лучшая закрытая задача конкретного дня. */
final case class DailyTopTask(date: LocalDate, task: TopTask)

/** Ответ `GET /analytics/most-expensive-task`: по дням окна + абсолютный максимум. */
final case class MostExpensiveTaskReport(items: List[DailyTopTask], overall: Option[TopTask])

/** Use case'ы Analytics Service. Один метод — одна операция. */
trait AnalyticsService:
  /** Доход менеджмента за диапазон дат (admin): `sum(assignFee) − sum(completeReward)` по дням из `daily_stats`. */
  def topManagementEarnings(
      from: LocalDate,
      to: LocalDate,
      actor: AuthenticatedUser
  ): ZIO[Any, DomainError, ManagementEarningsReport]

  /** Попуги с отрицательным балансом (admin). */
  def popugsInMinus(actor: AuthenticatedUser): ZIO[Any, DomainError, PopugsInMinusReport]

  /** Самая дорогая закрытая задача за скользящее окно от даты (admin): по 1 задаче на каждый день + overall. */
  def mostExpensiveTask(
      period: AnalyticsPeriod,
      date: LocalDate,
      actor: AuthenticatedUser
  ): ZIO[Any, DomainError, MostExpensiveTaskReport]

object AnalyticsService:
  /** Слой сервиса поверх read-side проекций. */
  val layer: ZLayer[AnalyticsStore, Nothing, AnalyticsService] =
    ZLayer.fromFunction(AnalyticsServiceLive(_))

/** Реализация AnalyticsService: проверка прав (admin), валидация запроса и агрегация проекций. */
final case class AnalyticsServiceLive(store: AnalyticsStore) extends AnalyticsService:
  def topManagementEarnings(
      from: LocalDate,
      to: LocalDate,
      actor: AuthenticatedUser
  ): ZIO[Any, DomainError, ManagementEarningsReport] =
    for
      _ <- requireAdmin(actor)
      _ <- requireRange(from, to)
      stats <- store.managementEarnings(from, to)
      items = stats.map(s => DailyEarnings(s.date, Money.fromCents(s.managementEarningsCents)))
      total = items.foldLeft(Money.zero)((acc, item) => acc + item.amount)
    yield ManagementEarningsReport(items, total)

  def popugsInMinus(actor: AuthenticatedUser): ZIO[Any, DomainError, PopugsInMinusReport] =
    for
      _ <- requireAdmin(actor)
      items <- store.popugsInMinus
    yield PopugsInMinusReport(items.size, items)

  def mostExpensiveTask(
      period: AnalyticsPeriod,
      date: LocalDate,
      actor: AuthenticatedUser
  ): ZIO[Any, DomainError, MostExpensiveTaskReport] =
    for
      _ <- requireAdmin(actor)
      (from, to) = period.windowFrom(date)
      tasks <- store.mostExpensiveTasks(startOfDay(from), startOfDay(to.plusDays(1)))
    yield buildReport(tasks)

  /** Доступ к аналитике: только admin. */
  private def requireAdmin(actor: AuthenticatedUser): ZIO[Any, DomainError, Unit] =
    actor.role match
      case Role.Admin => ZIO.unit
      case _          => ZIO.fail(AccessDenied("Admin privileges required"))

  /** Диапазон дат должен быть корректным: `from` не позже `to`. */
  private def requireRange(from: LocalDate, to: LocalDate): ZIO[Any, DomainError, Unit] =
    if from.isAfter(to) then ZIO.fail(InvalidValue("from", s"from ($from) must not be after to ($to)"))
    else ZIO.unit

  /** Начало дня в UTC. */
  private def startOfDay(date: LocalDate): Instant = date.atStartOfDay(ZoneOffset.UTC).toInstant

  /** Агрегирует закрытые задачи окна: лучшая за каждый день (детерминированно при равенстве сумм) + overall. */
  private def buildReport(tasks: List[CompletedTask]): MostExpensiveTaskReport =
    val perDay = tasks
      .groupBy(_.completedOn)
      .toList
      .map { case (day, dayTasks) =>
        DailyTopTask(day, bestOf(dayTasks))
      }
      .sortBy(_.date)
    MostExpensiveTaskReport(perDay, tasks.maxByOption(_.completeRewardCents).map(toTopTask))

  /** Лучшая задача из непустого списка: максимальный reward, при равенстве — лексикографически больший id
    * (детерминизм). Список гарантированно непуст (группа `groupBy`).
    */
  private def bestOf(tasks: List[CompletedTask]): TopTask =
    toTopTask(tasks.maxBy(task => (task.completeRewardCents, task.taskId.toString)))

  /** Маппит проекцию закрытой задачи в доменную запись ответа. */
  private def toTopTask(task: CompletedTask): TopTask =
    TopTask(task.taskId, task.title, Money.fromCents(task.completeRewardCents))
