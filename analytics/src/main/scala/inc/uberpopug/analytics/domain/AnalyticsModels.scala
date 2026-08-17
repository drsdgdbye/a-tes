package inc.uberpopug.analytics.domain

import java.time.LocalDate
import java.util.UUID

/** Строка ежедневной статистики (read из `daily_stats`). */
final case class DailyStat(date: LocalDate, managementEarningsCents: Long)

/** Попуг с отрицательным балансом для `GET /analytics/popugs-in-minus`. */
final case class PopugInMinus(userId: UUID, name: String, balanceCents: Long)

/** Закрытая задача для `GET /analytics/most-expensive-task`. */
final case class CompletedTask(taskId: UUID, title: String, completeRewardCents: Long, completedOn: LocalDate)
