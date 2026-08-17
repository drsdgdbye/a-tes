package inc.uberpopug.accounting.api

import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*

/** Декларативное описание HTTP-эндпоинтов Accounting Service через tapir. Личность берётся из `X-Auth-*` заголовков,
  * которые инжектит Gateway после верификации JWT.
  */
object AccountingEndpoints:
  import AccountingDtos.given

  /** Единый формат ошибки: HTTP-статус + JSON-тело `ErrorResponse`. */
  private val jsonErrorOut: EndpointOutput[(StatusCode, ErrorResponse)] =
    statusCode.and(jsonBody[ErrorResponse])

  /** Identity-заголовки, добавляемые Gateway: id и роль верифицированного пользователя. */
  private val identityIn: EndpointInput[(String, String)] =
    header[String]("X-Auth-User-Id").and(header[String]("X-Auth-User-Role"))

  /** `GET /health` — liveness-проверка. */
  val health: PublicEndpoint[Unit, Unit, HealthResponse, Any] =
    endpoint.get.in("health").out(jsonBody[HealthResponse])

  /** `GET /ready` — readiness-проверка. */
  val ready: PublicEndpoint[Unit, Unit, HealthResponse, Any] =
    endpoint.get.in("ready").out(jsonBody[HealthResponse])

  /** `GET /accounts/me/balance` — текущий баланс (все). */
  val balance: PublicEndpoint[(String, String), (StatusCode, ErrorResponse), BalanceResponse, Any] =
    endpoint.get
      .in(identityIn)
      .in("accounts" / "me" / "balance")
      .out(jsonBody[BalanceResponse])
      .errorOut(jsonErrorOut)

  /** `GET /accounts/me/audit-log` — страница аудитлога с пагинацией (все). */
  val auditLog: PublicEndpoint[
    (String, String, Option[Int], Option[Int]),
    (StatusCode, ErrorResponse),
    AuditLogResponse,
    Any
  ] =
    endpoint.get
      .in(identityIn)
      .in("accounts" / "me" / "audit-log")
      .in(query[Option[Int]]("limit"))
      .in(query[Option[Int]]("offset"))
      .out(jsonBody[AuditLogResponse])
      .errorOut(jsonErrorOut)

  /** `GET /accounts/top-management-earnings` — доход менеджмента за дату (admin/accountant). */
  val managementEarnings: PublicEndpoint[
    (String, String, String),
    (StatusCode, ErrorResponse),
    ManagementEarningsResponse,
    Any
  ] =
    endpoint.get
      .in(identityIn)
      .in("accounts" / "top-management-earnings")
      .in(query[String]("date"))
      .out(jsonBody[ManagementEarningsResponse])
      .errorOut(jsonErrorOut)

  /** `GET /accounts/daily-stats` — статистика за диапазон дат (admin/accountant). */
  val dailyStats: PublicEndpoint[
    (String, String, String, String),
    (StatusCode, ErrorResponse),
    DailyStatsResponse,
    Any
  ] =
    endpoint.get
      .in(identityIn)
      .in("accounts" / "daily-stats")
      .in(query[String]("from"))
      .in(query[String]("to"))
      .out(jsonBody[DailyStatsResponse])
      .errorOut(jsonErrorOut)

  /** Все эндпоинты сервиса для монтажа в HTTP-сервер. */
  val all: List[AnyEndpoint] =
    List(
      health,
      ready,
      balance,
      auditLog,
      managementEarnings,
      dailyStats
    )
