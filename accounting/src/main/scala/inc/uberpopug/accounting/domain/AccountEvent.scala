package inc.uberpopug.accounting.domain

import java.time.{Instant, LocalDate}
import java.util.UUID

import zio.json.{DeriveJsonCodec, JsonCodec, jsonDiscriminator, jsonHint}

import inc.uberpopug.common.domain.{Money, TaskId, UserId}

/** Доменные события Accounting Service (event store). Каждое событие неизменяемо и самодостаточно: баланс и аудитлог —
  * проекции, вычисляемые проигрыванием событий. `deltaCents` — вклад события в баланс (положительный — начисление,
  * отрицательный — списание/выплата).
  */
@jsonDiscriminator("type")
sealed trait AccountEvent:
  /** Уникальный id самого события (для `events.event_id`). */
  def eventId: UUID

  /** Момент события в UTC. */
  def timestamp: Instant

  /** Id счёта (агрегата), к которому относится событие. */
  def aggregateId: UUID

  /** Тип события для колонки `events.event_type`. */
  def eventType: String

  /** Вклад в баланс в центах: `sum(deltaCents) = баланс`. */
  def deltaCents: Long

object AccountEvent:
  /** Причина списания. */
  enum DebitReason(val wire: String):
    /** Стоимость ассайна, списанная при назначении задачи. */
    case TaskAssigned extends DebitReason("TaskAssigned")

  /** Причина начисления. */
  enum CreditReason(val wire: String):
    /** Возврат AssignFee старому исполнителю при реассайне. */
    case AssignmentRefund extends CreditReason("AssignmentRefund")

    /** CompleteReward исполнителю при выполнении задачи. */
    case TaskCompleted extends CreditReason("TaskCompleted")

  /** Цены задачи зафиксированы при её создании (Kafka: `TaskCreated`). Баланс не меняет. */
  @jsonHint("TaskPriceRecorded")
  final case class TaskPriceRecorded(
      eventId: UUID,
      timestamp: Instant,
      taskId: TaskId,
      userId: UserId,
      assignFee: Money,
      completeReward: Money
  ) extends AccountEvent:
    def aggregateId: UUID = userId.value

    def eventType: String = "TaskPriceRecorded"

    def deltaCents: Long = 0L

  /** Списание AssignFee (Kafka: `TaskAssigned`, первичный ассайн или реассайн). */
  @jsonHint("AccountDebited")
  final case class AccountDebited(
      eventId: UUID,
      timestamp: Instant,
      userId: UserId,
      amount: Money,
      taskId: TaskId,
      reason: DebitReason
  ) extends AccountEvent:
    def aggregateId: UUID = userId.value

    def eventType: String = "AccountDebited"

    def deltaCents: Long = -amount.toCents

  /** Начисление: возврат AssignFee при реассайне или CompleteReward при выполнении (Kafka: `TaskAssigned` /
    * `TaskCompleted`).
    */
  @jsonHint("AccountCredited")
  final case class AccountCredited(
      eventId: UUID,
      timestamp: Instant,
      userId: UserId,
      amount: Money,
      taskId: TaskId,
      reason: CreditReason
  ) extends AccountEvent:
    def aggregateId: UUID = userId.value

    def eventType: String = "AccountCredited"

    def deltaCents: Long = amount.toCents

  /** Выплата в конце дня (cron): `amount = max(0, balance)`, счёт обнуляется на сумму выплаты. */
  @jsonHint("AccountPayout")
  final case class AccountPayout(
      eventId: UUID,
      timestamp: Instant,
      userId: UserId,
      amount: Money,
      date: LocalDate
  ) extends AccountEvent:
    def aggregateId: UUID = userId.value

    def eventType: String = "AccountPayout"

    def deltaCents: Long = -amount.toCents

  /** Кодек для `Money`: в payload event store деньги хранятся в центах (`int64`) — как в БД и Protobuf. */
  given JsonCodec[Money] = JsonCodec.long.transform(Money.fromCents, _.toCents)

  /** Кодек для `UserId`: opaque тип поверх UUID. */
  given JsonCodec[UserId] = JsonCodec.uuid.transform(UserId(_), _.value)

  /** Кодек для `TaskId`: opaque тип поверх UUID. */
  given JsonCodec[TaskId] = JsonCodec.uuid.transform(TaskId(_), _.value)

  /** Кодек для `DebitReason`: wire-имя случая (enum с параметром не выводится автоматически). */
  given JsonCodec[DebitReason] = JsonCodec.string.transform(DebitReason.valueOf, _.wire)

  /** Кодек для `CreditReason`: wire-имя случая. */
  given JsonCodec[CreditReason] = JsonCodec.string.transform(CreditReason.valueOf, _.wire)

  /** Кодек ADT: дискриминатор `type` = имя подтипа, указанное в `@jsonHint`. */
  given JsonCodec[AccountEvent] = DeriveJsonCodec.gen
