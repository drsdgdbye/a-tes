package inc.uberpopug.taskservice.domain

import scala.util.Random

import inc.uberpopug.common.domain.Money

/** Чистые функции ценовой политики: случайные цены в доменных диапазонах из §3.3. Генератор передаётся извне для
  * детерминированных тестов.
  */
object PricingPolicy:
  /** Случайный assignFee в диапазоне `[$10.00, $20.00]` с шагом `$1.00`. */
  def generateAssignFee(random: Random): Money =
    Money.fromCents(Task.MinAssignFee.toCents + random.nextInt(11) * 100L)

  /** Случайный completeReward в диапазоне `[$20.00, $40.00]` с шагом `$1.00`. */
  def generateCompleteReward(random: Random): Money =
    Money.fromCents(Task.MinCompleteReward.toCents + random.nextInt(21) * 100L)
