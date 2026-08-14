package inc.uberpopug.auth.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Роль пользователя. Значение `wire` — wire-представление в API и JWT. */
enum Role(val wire: String):
  /** Исполнитель: создаёт и выполняет задачи. */
  case Popug extends Role("popug")

  /** Управленец: видит доход менеджмента. */
  case Manager extends Role("manager")

  /** Бухгалтер: управляет выплатами и балансами. */
  case Accountant extends Role("accountant")

  /** Администратор: управляет пользователями и ролями. */
  case Admin extends Role("admin")

object Role:
  /** Парсит роль из строки без учёта регистра; неизвестное значение — ошибка. */
  def from(value: String): Either[DomainError, Role] =
    Role.values
      .find(_.wire == value.trim.toLowerCase)
      .toRight(InvalidValue("role", s"Invalid role: '$value'"))
