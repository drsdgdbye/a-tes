package inc.uberpopug.auth.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Bcrypt-хэш пароля (60 символов), хранится вместо открытого пароля. */
opaque type PasswordHash = String

object PasswordHash:
  /** Длина валидного bcrypt-хэша в символах. */
  val BcryptLength = 60

  /** Smart-конструктор: проверяет, что значение — строка из 60 символов с префиксом `$2` (варианты `$2a`/`$2b`/`$2y`).
    */
  def from(value: String): Either[DomainError, PasswordHash] =
    if value.length != BcryptLength || !value.startsWith("$2") then
      Left(InvalidValue("passwordHash", "PasswordHash must be a 60-character bcrypt string"))
    else Right(value)

  /** Непроверяющий конструктор (для использования в репозиториях/тестах). */
  def apply(value: String): PasswordHash = value

  /** Доступ к сырому строковому значению. */
  extension (hash: PasswordHash) def value: String = hash
