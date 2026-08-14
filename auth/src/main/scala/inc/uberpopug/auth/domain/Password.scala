package inc.uberpopug.auth.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Пароль пользователя в открытом виде — только на границе сервиса. */
opaque type Password = String

object Password:
  /** Smart-конструктор: отвергает пустые пароли. */
  def from(value: String): Either[DomainError, Password] =
    if value.isEmpty then Left(InvalidValue("password", "Password must not be empty"))
    else Right(value)

  /** Доступ к сырому строковому значению. */
  extension (password: Password) def value: String = password
