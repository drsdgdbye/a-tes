package inc.uberpopug.common.domain

import scala.math.BigDecimal.RoundingMode

import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Деньги в долларах в домене, ровно 2 знака после запятой. В БД и Protobuf — центы (int64). */
opaque type Money = BigDecimal

object Money:
  private val Scale = 2

  def from(value: BigDecimal): Either[DomainError, Money] =
    if value.scale > Scale then
      Left(InvalidValue("amount", s"Money must have at most $Scale decimal places, got: $value"))
    else Right(value.setScale(Scale))

  def fromCents(cents: Long): Money =
    BigDecimal(cents).setScale(Scale, RoundingMode.UNNECESSARY) / 100

  def zero: Money = BigDecimal(0).setScale(Scale)

  extension (money: Money)
    def +(other: Money): Money = (money + other).setScale(Scale)

    def -(other: Money): Money = (money - other).setScale(Scale)

    def *(factor: BigDecimal): Money =
      (money * factor).setScale(Scale, RoundingMode.HALF_UP)

    def toCents: Long =
      (money.setScale(Scale, RoundingMode.UNNECESSARY) * 100).toLongExact

    def isPositive: Boolean = money > 0

    def isNegative: Boolean = money < 0

    def isZero: Boolean = money == 0

    def value: BigDecimal = money

    def >(other: Money): Boolean = money > other

    def >=(other: Money): Boolean = money >= other

    def <(other: Money): Boolean = money < other

    def <=(other: Money): Boolean = money <= other
