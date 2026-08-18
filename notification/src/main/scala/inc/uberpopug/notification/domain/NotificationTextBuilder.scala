package inc.uberpopug.notification.domain

import java.time.LocalDate

import inc.uberpopug.common.domain.Money

/** Чистый билдер текста уведомлений (SSOT форматов сообщений, AGENTS §3). Единый текст для всех каналов; канальные
  * особенности рендеринга (например, лимит длины SMS) — будущая точка расширения.
  */
object NotificationTextBuilder:
  /** Текст сообщения для доменного события. */
  def render(event: NotificationEvent): String =
    event match
      case NotificationEvent.TaskAssigned(_, taskTitle, _)          => assigned(taskTitle)
      case NotificationEvent.TaskCompleted(_, taskTitle, reward, _) => completed(taskTitle, reward)
      case NotificationEvent.PaymentProcessed(_, amount, date, _)   => payment(amount, date)

  /** `TaskAssigned`: уведомление новому исполнителю. */
  def assigned(taskTitle: String): String =
    s"Вам назначена задача «$taskTitle»"

  /** `TaskCompleted`: уведомление исполнителю с суммой начисления. */
  def completed(taskTitle: String, rewardCents: Long): String =
    s"Задача «$taskTitle» выполнена. Начислено: ${formatMoney(rewardCents)}"

  /** `PaymentProcessed`: формат зафиксирован спекой — «Выплата за DD.MM: $XX». */
  def payment(amountCents: Long, date: LocalDate): String =
    s"Выплата за ${formatDate(date)}: ${formatMoney(amountCents)}"

  /** Форматирует центы как доллары: `1234` → `$12.34`. */
  private def formatMoney(cents: Long): String =
    s"$$${Money.fromCents(cents).value}"

  /** Форматирует дату как `DD.MM`. */
  private def formatDate(date: LocalDate): String =
    f"${date.getDayOfMonth}%02d.${date.getMonthValue}%02d"
