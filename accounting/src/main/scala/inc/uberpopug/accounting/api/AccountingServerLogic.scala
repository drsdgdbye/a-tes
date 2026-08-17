package inc.uberpopug.accounting.api

import java.time.LocalDate

import sttp.model.StatusCode
import sttp.tapir.ztapir.*
import zio.{Clock, ZIO, ZLayer}

import inc.uberpopug.accounting.domain.{AuditLogEntry, Role}
import inc.uberpopug.accounting.service.{AccountingService, AuthenticatedUser}
import inc.uberpopug.common.domain.{DomainError, Pagination, UserId}
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Server logic: связывает tapir-эндпоинты с AccountingService и маппингом ошибок. Личность достаётся из `X-Auth-*`
  * заголовков Gateway.
  */
final case class AccountingServerLogic(service: AccountingService):
  import AccountingEndpoints.*
  import AccountingServerLogic.*

  /** Восстанавливает верифицированную личность из identity-заголовков. */
  private def identity(userId: String, role: String): ZIO[Any, (StatusCode, ErrorResponse), AuthenticatedUser] =
    for
      uid <- ZIO.fromEither(UserId.from(userId)).mapError(ErrorMapping.toApiError)
      r <- ZIO.fromEither(Role.from(role)).mapError(ErrorMapping.toApiError)
    yield AuthenticatedUser(uid, r)

  /** Парсит ISO-дату из query-параметра; невалидная дата — `VALIDATION_ERROR`. */
  private def parseDate(value: String, field: String): ZIO[Any, (StatusCode, ErrorResponse), LocalDate] =
    ZIO
      .attempt(LocalDate.parse(value))
      .mapError(_ => ErrorMapping.toApiError(InvalidValue(field, s"Invalid ISO-8601 date: '$value'")))

  /** Все эндпоинты сервиса: публичные health/ready и защищённые через identity-заголовки. */
  val endpoints: List[ZServerEndpoint[Clock, Any]] =
    List(
      health.zServerLogic[Clock](_ => ZIO.succeed(HealthResponse("ok"))),
      ready.zServerLogic[Clock](_ => ZIO.succeed(HealthResponse("ok"))),
      balance.zServerLogic[Clock] { case (userId, role) =>
        for
          actor <- identity(userId, role)
          snap <- service.balanceOf(actor).mapError(ErrorMapping.toApiError)
        yield BalanceResponse(snap.userId.value.toString, snap.balance.value.toString, snap.date.toString)
      },
      auditLog.zServerLogic[Clock] { case (userId, role, limit, offset) =>
        for
          actor <- identity(userId, role)
          pagination = Pagination.from(limit, offset)
          (items, total) <-
            service.auditLog(actor, pagination.limit, pagination.offset).mapError(ErrorMapping.toApiError)
        yield AuditLogResponse(items.map(toAuditLogItem), total)
      },
      managementEarnings.zServerLogic[Clock] { case (userId, role, dateStr) =>
        for
          actor <- identity(userId, role)
          date <- parseDate(dateStr, "date")
          amount <- service.managementEarnings(date, actor).mapError(ErrorMapping.toApiError)
        yield ManagementEarningsResponse(amount.value.toString, date.toString)
      },
      dailyStats.zServerLogic[Clock] { case (userId, role, fromStr, toStr) =>
        for
          actor <- identity(userId, role)
          from <- parseDate(fromStr, "from")
          to <- parseDate(toStr, "to")
          stats <- service.dailyStats(from, to, actor).mapError(ErrorMapping.toApiError)
        yield DailyStatsResponse(stats.map(toDailyStat))
      }
    )

object AccountingServerLogic:
  /** Маппит доменную запись аудитлога в DTO. */
  def toAuditLogItem(entry: AuditLogEntry): AuditLogItemResponse =
    AuditLogItemResponse(
      id = entry.id.toString,
      `type` = entry.`type`,
      amount = entry.amount.value.toString,
      taskId = entry.taskId.map(_.toString),
      description = entry.description,
      timestamp = entry.timestamp
    )

  /** Маппит доменную запись ежедневной статистики в DTO. */
  def toDailyStat(stat: inc.uberpopug.accounting.domain.DailyStats): DailyStatResponse =
    DailyStatResponse(
      date = stat.date.toString,
      earnings = stat.earnings.value.toString,
      popugsTotal = stat.popugsTotal,
      popugsNegative = stat.popugsNegative
    )

  /** Слой server logic поверх AccountingService. */
  val layer: ZLayer[AccountingService, Nothing, AccountingServerLogic] =
    ZLayer.fromFunction(AccountingServerLogic(_))
