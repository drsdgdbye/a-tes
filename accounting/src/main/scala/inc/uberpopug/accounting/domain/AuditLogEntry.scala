package inc.uberpopug.accounting.domain

import java.time.Instant
import java.util.UUID

import inc.uberpopug.common.domain.Money

/** Запись аудитлога: одна финансовая операция по счёту. Проекция события event store. */
final case class AuditLogEntry(
    id: UUID,
    `type`: String,
    amount: Money,
    taskId: Option[UUID],
    description: String,
    timestamp: Instant
)

object AuditLogEntry:
  /** Тип записи: операция, списывающая AssignFee при ассайне. */
  val TypeAssign = "assign"

  /** Тип записи: начисление CompleteReward при выполнении. */
  val TypeComplete = "complete"

  /** Тип записи: возврат AssignFee старому исполнителю при реассайне. */
  val TypeRefund = "refund"

  /** Тип записи: ежедневная выплата. */
  val TypePayout = "payout"

  /** Проецирует финансовое событие счёта в запись аудитлога с человекочитаемым описанием. `TaskPriceRecorded` (цены
    * задачи) финансовой операцией не является и в аудитлог не попадает.
    */
  def fromEvent(event: AccountEvent): Option[AuditLogEntry] =
    event match
      case debit: AccountEvent.AccountDebited =>
        Some(
          AuditLogEntry(
            id = debit.eventId,
            `type` = TypeAssign,
            amount = debit.amount,
            taskId = Some(debit.taskId.value),
            description = s"Task ${debit.taskId.value} assigned: assign fee charged",
            timestamp = debit.timestamp
          )
        )
      case credit: AccountEvent.AccountCredited =>
        credit.reason match
          case AccountEvent.CreditReason.AssignmentRefund =>
            Some(
              AuditLogEntry(
                id = credit.eventId,
                `type` = TypeRefund,
                amount = credit.amount,
                taskId = Some(credit.taskId.value),
                description = s"Task ${credit.taskId.value} reassigned: assign fee refunded",
                timestamp = credit.timestamp
              )
            )
          case AccountEvent.CreditReason.TaskCompleted =>
            Some(
              AuditLogEntry(
                id = credit.eventId,
                `type` = TypeComplete,
                amount = credit.amount,
                taskId = Some(credit.taskId.value),
                description = s"Task ${credit.taskId.value} completed: reward credited",
                timestamp = credit.timestamp
              )
            )
      case payout: AccountEvent.AccountPayout =>
        Some(
          AuditLogEntry(
            id = payout.eventId,
            `type` = TypePayout,
            amount = payout.amount,
            taskId = None,
            description = s"Daily payout for ${payout.date}: ${payout.amount.value}",
            timestamp = payout.timestamp
          )
        )
      case _: AccountEvent.TaskPriceRecorded => None
