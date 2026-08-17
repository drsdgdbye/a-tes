package inc.uberpopug.accounting.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Роль пользователя, приходящая в `X-Auth-User-Role` от Gateway. Локальная копия `auth.domain.Role`: вынос в общий
  * модуль — вне рамок текущей версии.
  */
enum Role(val wire: String):
  /** Исполнитель: видит только свой баланс и аудитлог. */
  case Popug extends Role("popug")

  /** Управленец: доступ к отчётам менеджмента. */
  case Manager extends Role("manager")

  /** Бухгалтер: доступ к отчётам менеджмента и ежедневной статистике. */
  case Accountant extends Role("accountant")

  /** Администратор: полный доступ. */
  case Admin extends Role("admin")

object Role:
  /** Парсит роль из строки без учёта регистра; неизвестное значение — ошибка. */
  def from(value: String): Either[DomainError, Role] =
    Role.values
      .find(_.wire == value.trim.toLowerCase)
      .toRight(InvalidValue("role", s"Invalid role: '$value'"))
