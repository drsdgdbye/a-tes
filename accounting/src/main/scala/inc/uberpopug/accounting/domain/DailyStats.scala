package inc.uberpopug.accounting.domain

import java.time.LocalDate

import inc.uberpopug.common.domain.Money

/** Доход менеджмента за день: `sum(assignFee) − sum(completeReward)` по задачам, созданным в этот день. Может быть
  * отрицательным.
  */
object ManagementEarnings:
  /** Считает доход менеджмента за день по событиям `TaskPriceRecorded` этого дня. */
  def forDate(events: List[AccountEvent], date: LocalDate): Money =
    events
      .collect {
        case recorded: AccountEvent.TaskPriceRecorded if BalanceCalculator.isOnDay(recorded, date) =>
          recorded.assignFee - recorded.completeReward
      }
      .foldLeft(Money.zero)(_ + _)

/** Строка ежедневной статистики для `GET /accounts/daily-stats`. */
final case class DailyStats(date: LocalDate, earnings: Money, popugsTotal: Int, popugsNegative: Int)

/** Строит ежедневную статистику за диапазон дат (включительно) по событиям и проекции пользователей. Баланс каждого
  * попуга на конец дня — проигрывание всех событий, произошедших строго до конца дня.
  */
object DailyStatsCalculator:
  /** Пользователь проекции: id, имя и момент создания счёта. */
  final case class UserSnapshot(userId: java.util.UUID, name: String, createdAt: java.time.Instant)

  /** Строит статистику по отсортированным по времени событиям и пользователям. `users` — полная проекция (все
    * зарегистрированные попуги).
    */
  def build(
      from: LocalDate,
      to: LocalDate,
      events: List[AccountEvent],
      users: List[UserSnapshot]
  ): List[DailyStats] =
    val ordered = events.sortBy(_.timestamp)
    loop(dateRange(from, to), ordered, Map.empty[java.util.UUID, Money], Nil, users)

  /** Диапазон дат от `from` до `to` включительно. */
  def dateRange(from: LocalDate, to: LocalDate): List[LocalDate] =
    Iterator.iterate(from)(_.plusDays(1)).takeWhile(!_.isAfter(to)).toList

  /** Проходит по дням, потребляя события по мере приближения к границе каждого дня. */
  @scala.annotation.tailrec
  private def loop(
      dates: List[LocalDate],
      pending: List[AccountEvent],
      balances: Map[java.util.UUID, Money],
      acc: List[DailyStats],
      users: List[UserSnapshot]
  ): List[DailyStats] =
    dates match
      case Nil         => acc.reverse
      case day :: rest =>
        val end = BalanceCalculator.endOfDay(day)
        val (today, later) = pending.span(event => event.timestamp.isBefore(end))
        val updated = today.foldLeft(balances) { (map, event) =>
          map.updated(
            event.aggregateId,
            map.getOrElse(event.aggregateId, Money.zero) + Money.fromCents(event.deltaCents)
          )
        }
        val earnings =
          today
            .collect { case recorded: AccountEvent.TaskPriceRecorded => recorded.assignFee - recorded.completeReward }
            .foldLeft(Money.zero)(_ + _)
        val popugsTotal = users.count(_.createdAt.isBefore(end))
        val popugsNegative = updated.values.count(_.isNegative)
        loop(rest, later, updated, DailyStats(day, earnings, popugsTotal, popugsNegative) :: acc, users)
