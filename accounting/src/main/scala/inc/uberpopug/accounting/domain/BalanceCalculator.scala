package inc.uberpopug.accounting.domain

import java.time.{Instant, LocalDate}
import java.time.ZoneOffset

import inc.uberpopug.common.domain.Money

/** Чистые расчётные функции баланса счёта по событиям (проекция event store). */
object BalanceCalculator:
  /** Начало дня в UTC: `YYYY-MM-DDT00:00:00Z`. */
  def startOfDay(date: LocalDate): Instant = date.atStartOfDay(ZoneOffset.UTC).toInstant

  /** Конец дня в UTC: исключающая граница `YYYY-MM-DDT00:00:00Z` следующего дня. */
  def endOfDay(date: LocalDate): Instant = date.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant

  /** Входит ли событие в указанный день (UTC). */
  def isOnDay(event: AccountEvent, date: LocalDate): Boolean =
    !event.timestamp.isBefore(startOfDay(date)) && event.timestamp.isBefore(endOfDay(date))

  /** Текущий баланс: сумма `deltaCents` всех событий. Симметричность `sum(events) = balance` — инвариант event store.
    */
  def currentBalance(events: List[AccountEvent]): Money =
    events.foldLeft(Money.zero)((acc, event) => acc + Money.fromCents(event.deltaCents))

  /** Баланс счёта на указанную дату: только события этого дня. */
  def dailyBalance(events: List[AccountEvent], date: LocalDate): Money =
    events
      .filter(event => isOnDay(event, date))
      .foldLeft(Money.zero)((acc, event) => acc + Money.fromCents(event.deltaCents))
