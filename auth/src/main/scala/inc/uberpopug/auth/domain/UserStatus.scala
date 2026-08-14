package inc.uberpopug.auth.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Статус пользователя: активен или отключён (мягкое удаление). */
enum UserStatus(val wire: String):
  /** Пользователь может логиниться и выполнять задачи. */
  case Active extends UserStatus("active")

  /** Пользователь отключён: вход запрещён, refresh-токены отозваны. */
  case Disabled extends UserStatus("disabled")

object UserStatus:
  /** Парсит статус из строки без учёта регистра; неизвестное значение — ошибка. */
  def from(value: String): Either[DomainError, UserStatus] =
    UserStatus.values
      .find(_.wire == value.trim.toLowerCase)
      .toRight(InvalidValue("status", s"Invalid status: '$value'"))
