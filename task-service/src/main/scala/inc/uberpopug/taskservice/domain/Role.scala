package inc.uberpopug.taskservice.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Роль пользователя, приходящая в `X-Auth-User-Role` от Gateway. Локальная копия `auth.domain.Role`: вынос в общий
  * модуль — вне рамок текущей версии.
  */
enum Role(val wire: String):
  /** Исполнитель: создаёт и выполняет задачи. */
  case Popug extends Role("popug")

  /** Управленец: может запускать перетасовку. */
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
