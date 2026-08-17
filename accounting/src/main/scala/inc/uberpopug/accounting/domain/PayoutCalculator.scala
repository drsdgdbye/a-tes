package inc.uberpopug.accounting.domain

import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.util.UUID

import inc.uberpopug.common.domain.{Money, UserId}

/** Чистые функции расчёта выплаты в конце дня. */
object PayoutCalculator:
  /** Сумма выплаты по балансу: `max(0, balance)`. Отрицательный баланс — выплата 0, долг переносится на следующий день.
    */
  def payoutAmount(balance: Money): Money =
    if balance.isPositive then balance else Money.zero

  /** Детерминированный id события выплаты: `nameUUIDFromBytes(userId:date)`. Повторный запуск cron в тот же день даёт
    * тот же id — идемпотентность (M-ACC-19).
    */
  def payoutEventId(userId: UserId, date: LocalDate): UUID =
    UUID.nameUUIDFromBytes(s"${userId.value}:$date".getBytes(StandardCharsets.UTF_8))
