package inc.uberpopug.analytics.api

import java.time.LocalDate

import sttp.model.StatusCode
import sttp.tapir.ztapir.*
import zio.{Clock, ZIO, ZLayer}

import inc.uberpopug.analytics.domain.AnalyticsPeriod
import inc.uberpopug.analytics.service.{AnalyticsService, AuthenticatedUser}
import inc.uberpopug.common.domain.{DomainError, UserId}
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Server logic: связывает tapir-эндпоинты с AnalyticsService и маппингом ошибок. Личность достаётся из `X-Auth-*`
  * заголовков Gateway.
  */
final case class AnalyticsServerLogic(service: AnalyticsService):
  import AnalyticsEndpoints.*
  import AnalyticsServerLogic.*

  /** Восстанавливает верифицированную личность из identity-заголовков. */
  private def identity(userId: String, role: String): ZIO[Any, (StatusCode, ErrorResponse), AuthenticatedUser] =
    for
      uid <- ZIO.fromEither(UserId.from(userId)).mapError(ErrorMapping.toApiError)
      r <- ZIO.fromEither(inc.uberpopug.analytics.domain.Role.from(role)).mapError(ErrorMapping.toApiError)
    yield AuthenticatedUser(uid, r)

  /** Парсит ISO-дату из query-параметра; невалидная дата — `VALIDATION_ERROR`. */
  private def parseDate(value: String, field: String): ZIO[Any, (StatusCode, ErrorResponse), LocalDate] =
    ZIO
      .attempt(LocalDate.parse(value))
      .mapError(_ => ErrorMapping.toApiError(InvalidValue(field, s"Invalid ISO-8601 date: '$value'")))

  /** Парсит период агрегации; невалидное значение — `VALIDATION_ERROR`. */
  private def parsePeriod(value: String): ZIO[Any, (StatusCode, ErrorResponse), AnalyticsPeriod] =
    ZIO.fromEither(AnalyticsPeriod.from(value)).mapError(ErrorMapping.toApiError)

  /** Все эндпоинты сервиса: публичные health/ready и защищённые через identity-заголовки. */
  val endpoints: List[ZServerEndpoint[Clock, Any]] =
    List(
      health.zServerLogic[Clock](_ => ZIO.succeed(HealthResponse("ok"))),
      ready.zServerLogic[Clock](_ => ZIO.succeed(HealthResponse("ok"))),
      topManagementEarnings.zServerLogic[Clock] { case (userId, role, fromStr, toStr) =>
        for
          actor <- identity(userId, role)
          from <- parseDate(fromStr, "from")
          to <- parseDate(toStr, "to")
          report <- service.topManagementEarnings(from, to, actor).mapError(ErrorMapping.toApiError)
        yield TopManagementEarningsResponse(report.items.map(toEarningsItem), report.total.value.toString)
      },
      popugsInMinus.zServerLogic[Clock] { case (userId, role) =>
        for
          actor <- identity(userId, role)
          report <- service.popugsInMinus(actor).mapError(ErrorMapping.toApiError)
        yield PopugsInMinusResponse(report.count, report.items.map(toMinusItem))
      },
      mostExpensiveTask.zServerLogic[Clock] { case (userId, role, periodStr, dateStr) =>
        for
          actor <- identity(userId, role)
          period <- parsePeriod(periodStr)
          date <- parseDate(dateStr, "date")
          report <- service.mostExpensiveTask(period, date, actor).mapError(ErrorMapping.toApiError)
        yield MostExpensiveTaskResponse(report.items.map(toExpensiveItem), report.overall.map(toTopTask))
      }
    )

object AnalyticsServerLogic:
  /** Маппит доменную запись дохода менеджмента в DTO. */
  def toEarningsItem(item: inc.uberpopug.analytics.service.DailyEarnings): TopManagementEarningsItem =
    TopManagementEarningsItem(item.date.toString, item.amount.value.toString)

  /** Маппит доменную запись попуга в минусе в DTO. */
  def toMinusItem(item: inc.uberpopug.analytics.domain.PopugInMinus): PopugInMinusItem =
    PopugInMinusItem(
      item.userId.toString,
      item.name,
      inc.uberpopug.common.domain.Money.fromCents(item.balanceCents).value.toString
    )

  /** Маппит доменную запись лучшей задачи дня в DTO. */
  def toExpensiveItem(item: inc.uberpopug.analytics.service.DailyTopTask): MostExpensiveTaskItem =
    MostExpensiveTaskItem(
      item.date.toString,
      item.task.taskId.toString,
      item.task.title,
      item.task.amount.value.toString
    )

  /** Маппит доменную запись абсолютной лучшей задачи в DTO. */
  def toTopTask(task: inc.uberpopug.analytics.service.TopTask): TopTaskItem =
    TopTaskItem(task.taskId.toString, task.title, task.amount.value.toString)

  /** Слой server logic поверх AnalyticsService. */
  val layer: ZLayer[AnalyticsService, Nothing, AnalyticsServerLogic] =
    ZLayer.fromFunction(AnalyticsServerLogic(_))
