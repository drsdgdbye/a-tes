package inc.uberpopug.accounting.service

import java.time.LocalDate

import zio.{Clock, ZIO, ZLayer}

import inc.uberpopug.accounting.domain.{
  AuditLogEntry,
  BalanceCalculator,
  DailyStats,
  DailyStatsCalculator,
  ManagementEarnings,
  Role
}
import inc.uberpopug.accounting.repository.EventStore
import inc.uberpopug.common.domain.{DomainError, Money, UserId}
import inc.uberpopug.common.domain.DomainError.{AccountNotFound, AccessDenied, InvalidValue}

/** Текущий баланс счёта и дата снапшота. */
final case class BalanceSnapshot(userId: UserId, balance: Money, date: LocalDate)

/** Use case'ы Accounting Service. Один метод — одна операция. */
trait AccountingService:
  /** Текущий баланс своего счёта (все роли). */
  def balanceOf(actor: AuthenticatedUser): ZIO[Clock, DomainError, BalanceSnapshot]

  /** Страница аудитлога своего счёта (все роли). */
  def auditLog(actor: AuthenticatedUser, limit: Int, offset: Int): ZIO[Any, DomainError, (List[AuditLogEntry], Long)]

  /** Доход менеджмента за день (admin/accountant): sum(assignFee) − sum(completeReward) по созданным в этот день
    * задачам.
    */
  def managementEarnings(date: LocalDate, actor: AuthenticatedUser): ZIO[Any, DomainError, Money]

  /** Ежедневная статистика за диапазон дат (admin/accountant). */
  def dailyStats(from: LocalDate, to: LocalDate, actor: AuthenticatedUser): ZIO[Any, DomainError, List[DailyStats]]

object AccountingService:
  /** Слой сервиса поверх event store. Clock подставляется на месте вызова. */
  val layer: ZLayer[EventStore, Nothing, AccountingService] =
    ZLayer.fromFunction(AccountingServiceLive(_))

/** Реализация AccountingService: оркестрация event store и чистых расчётных политик. */
final case class AccountingServiceLive(store: EventStore) extends AccountingService:
  def balanceOf(actor: AuthenticatedUser): ZIO[Clock, DomainError, BalanceSnapshot] =
    for
      now <- Clock.instant
      balance <- store.balanceOf(actor.id).flatMap {
        case Some(value) => ZIO.succeed(value)
        case None        => ZIO.fail(AccountNotFound(actor.id.value.toString))
      }
    yield BalanceSnapshot(actor.id, balance, now.atZone(java.time.ZoneOffset.UTC).toLocalDate)

  def auditLog(actor: AuthenticatedUser, limit: Int, offset: Int): ZIO[Any, DomainError, (List[AuditLogEntry], Long)] =
    store.auditLog(actor.id, limit, offset)

  def managementEarnings(date: LocalDate, actor: AuthenticatedUser): ZIO[Any, DomainError, Money] =
    for
      _ <- requireAdminOrAccountant(actor)
      events <- store.eventsBetween(BalanceCalculator.startOfDay(date), BalanceCalculator.endOfDay(date))
    yield ManagementEarnings.forDate(events, date)

  def dailyStats(from: LocalDate, to: LocalDate, actor: AuthenticatedUser): ZIO[Any, DomainError, List[DailyStats]] =
    for
      _ <- requireAdminOrAccountant(actor)
      _ <-
        if from.isAfter(to) then ZIO.fail(InvalidValue("from", s"from ($from) must not be after to ($to)"))
        else ZIO.unit
      events <- store.eventsBefore(BalanceCalculator.endOfDay(to))
      users <- store.listUsers
    yield DailyStatsCalculator.build(from, to, events, users)

  /** Доступ к отчётам менеджмента и статистике: только admin и accountant. */
  private def requireAdminOrAccountant(actor: AuthenticatedUser): ZIO[Any, DomainError, Unit] =
    actor.role match
      case Role.Admin | Role.Accountant => ZIO.unit
      case _                            => ZIO.fail(AccessDenied("Admin or accountant privileges required"))
