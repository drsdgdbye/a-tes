package inc.uberpopug.analytics.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Роль пользователя, приходящая в `X-Auth-User-Role` от Gateway. Локальная копия `auth.domain.Role`: вынос в общий
  * модуль — вне рамок текущей версии.
  */
enum Role(val wire: String):
  /** Исполнитель: аналитика не доступна. */
  case Popug extends Role("popug")

  /** Управленец: аналитика не доступна (только admin). */
  case Manager extends Role("manager")

  /** Бухгалтер: аналитика не доступна (только admin). */
  case Accountant extends Role("accountant")

  /** Администратор: полный доступ к аналитике. */
  case Admin extends Role("admin")

object Role:
  /** Парсит роль из строки без учёта регистра; неизвестное значение — ошибка. */
  def from(value: String): Either[DomainError, Role] =
    Role.values
      .find(_.wire == value.trim.toLowerCase)
      .toRight(InvalidValue("role", s"Invalid role: '$value'"))
