package inc.uberpopug.analytics.api

import zio.json.{DeriveJsonCodec, JsonCodec}

/** Один день дохода менеджмента. */
final case class TopManagementEarningsItem(date: String, amount: String)

/** Ответ `GET /analytics/top-management-earnings`. */
final case class TopManagementEarningsResponse(items: List[TopManagementEarningsItem], total: String)

/** Попуг с отрицательным балансом. */
final case class PopugInMinusItem(userId: String, name: String, balance: String)

/** Ответ `GET /analytics/popugs-in-minus`. */
final case class PopugsInMinusResponse(count: Int, items: List[PopugInMinusItem])

/** Лучшая закрытая задача. */
final case class TopTaskItem(taskId: String, title: String, amount: String)

/** Лучшая закрытая задача конкретного дня. */
final case class MostExpensiveTaskItem(date: String, taskId: String, title: String, amount: String)

/** Ответ `GET /analytics/most-expensive-task`. */
final case class MostExpensiveTaskResponse(items: List[MostExpensiveTaskItem], overall: Option[TopTaskItem])

/** Ответ liveness/readiness эндпоинтов. */
final case class HealthResponse(status: String)

object AnalyticsDtos:
  given JsonCodec[ErrorResponse] = DeriveJsonCodec.gen
  given JsonCodec[TopManagementEarningsItem] = DeriveJsonCodec.gen
  given JsonCodec[TopManagementEarningsResponse] = DeriveJsonCodec.gen
  given JsonCodec[PopugInMinusItem] = DeriveJsonCodec.gen
  given JsonCodec[PopugsInMinusResponse] = DeriveJsonCodec.gen
  given JsonCodec[TopTaskItem] = DeriveJsonCodec.gen
  given JsonCodec[MostExpensiveTaskItem] = DeriveJsonCodec.gen
  given JsonCodec[MostExpensiveTaskResponse] = DeriveJsonCodec.gen
  given JsonCodec[HealthResponse] = DeriveJsonCodec.gen
