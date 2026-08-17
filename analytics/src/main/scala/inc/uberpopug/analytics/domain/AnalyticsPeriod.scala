package inc.uberpopug.analytics.domain

import java.time.LocalDate

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Период агрегации для `GET /analytics/most-expensive-task`: задаёт скользящее окно от опорной даты. */
enum AnalyticsPeriod(val wire: String):
  /** Один день: `[D, D]`. */
  case Day extends AnalyticsPeriod("day")

  /** Неделя: `[D, D+6]`. */
  case Week extends AnalyticsPeriod("week")

  /** Месяц: `[D, D+29]`. */
  case Month extends AnalyticsPeriod("month")

object AnalyticsPeriod:
  /** Парсит период из строки; неизвестное значение — ошибка. */
  def from(value: String): Either[DomainError, AnalyticsPeriod] =
    AnalyticsPeriod.values
      .find(_.wire == value.trim.toLowerCase)
      .toRight(InvalidValue("period", s"Invalid period: '$value'"))

  /** Скользящее окно (включительно) от опорной даты: `day = [D, D]`, `week = [D, D+6]`, `month = [D, D+29]`. */
  extension (period: AnalyticsPeriod)
    def windowFrom(date: LocalDate): (LocalDate, LocalDate) =
      period match
        case AnalyticsPeriod.Day   => (date, date)
        case AnalyticsPeriod.Week  => (date, date.plusDays(6))
        case AnalyticsPeriod.Month => (date, date.plusDays(29))
