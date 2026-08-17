package inc.uberpopug.analytics.api

import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*

/** Декларативное описание HTTP-эндпоинтов Analytics Service через tapir. Личность берётся из `X-Auth-*` заголовков,
  * которые инжектит Gateway после верификации JWT.
  */
object AnalyticsEndpoints:
  import AnalyticsDtos.given

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

  /** `GET /analytics/top-management-earnings` — доход менеджмента за диапазон дат (admin). */
  val topManagementEarnings: PublicEndpoint[
    (String, String, String, String),
    (StatusCode, ErrorResponse),
    TopManagementEarningsResponse,
    Any
  ] =
    endpoint.get
      .in(identityIn)
      .in("analytics" / "top-management-earnings")
      .in(query[String]("from"))
      .in(query[String]("to"))
      .out(jsonBody[TopManagementEarningsResponse])
      .errorOut(jsonErrorOut)

  /** `GET /analytics/popugs-in-minus` — попуги с отрицательным балансом (admin). */
  val popugsInMinus: PublicEndpoint[(String, String), (StatusCode, ErrorResponse), PopugsInMinusResponse, Any] =
    endpoint.get
      .in(identityIn)
      .in("analytics" / "popugs-in-minus")
      .out(jsonBody[PopugsInMinusResponse])
      .errorOut(jsonErrorOut)

  /** `GET /analytics/most-expensive-task` — самая дорогая закрытая задача за период (admin). */
  val mostExpensiveTask: PublicEndpoint[
    (String, String, String, String),
    (StatusCode, ErrorResponse),
    MostExpensiveTaskResponse,
    Any
  ] =
    endpoint.get
      .in(identityIn)
      .in("analytics" / "most-expensive-task")
      .in(query[String]("period"))
      .in(query[String]("date"))
      .out(jsonBody[MostExpensiveTaskResponse])
      .errorOut(jsonErrorOut)

  /** Все эндпоинты сервиса для монтажа в HTTP-сервер. */
  val all: List[AnyEndpoint] =
    List(
      health,
      ready,
      topManagementEarnings,
      popugsInMinus,
      mostExpensiveTask
    )
