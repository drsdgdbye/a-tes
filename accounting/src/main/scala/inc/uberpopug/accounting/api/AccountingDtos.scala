package inc.uberpopug.accounting.api

import java.time.Instant

import zio.json.{DeriveJsonCodec, JsonCodec}

/** Ответ `GET /accounts/me/balance` — текущий баланс и дата снапшота. */
final case class BalanceResponse(userId: String, balance: String, date: String)

/** Одна запись аудитлога. Тип — одно из `assign` / `complete` / `refund` / `payout`. */
final case class AuditLogItemResponse(
    id: String,
    `type`: String,
    amount: String,
    taskId: Option[String],
    description: String,
    timestamp: Instant
)

/** Ответ `GET /accounts/me/audit-log` — страница записей. */
final case class AuditLogResponse(items: List[AuditLogItemResponse], total: Long)

/** Ответ `GET /accounts/top-management-earnings` — доход менеджмента за дату. */
final case class ManagementEarningsResponse(amount: String, date: String)

/** Одна запись ежедневной статистики. */
final case class DailyStatResponse(date: String, earnings: String, popugsTotal: Int, popugsNegative: Int)

/** Ответ `GET /accounts/daily-stats` — статистика за диапазон дат. */
final case class DailyStatsResponse(items: List[DailyStatResponse])

/** Ответ liveness/readiness эндпоинтов. */
final case class HealthResponse(status: String)

object AccountingDtos:
  given JsonCodec[ErrorResponse] = DeriveJsonCodec.gen
  given JsonCodec[BalanceResponse] = DeriveJsonCodec.gen
  given JsonCodec[AuditLogItemResponse] = DeriveJsonCodec.gen
  given JsonCodec[AuditLogResponse] = DeriveJsonCodec.gen
  given JsonCodec[ManagementEarningsResponse] = DeriveJsonCodec.gen
  given JsonCodec[DailyStatResponse] = DeriveJsonCodec.gen
  given JsonCodec[DailyStatsResponse] = DeriveJsonCodec.gen
  given JsonCodec[HealthResponse] = DeriveJsonCodec.gen
