package inc.uberpopug.auth.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Электронная почта пользователя. Непустая и содержащая `@`. */
opaque type Email = String

object Email:
  /** Smart-конструктор: принимает email, обрезает пробелы и проверяет, что значение непустое и содержит `@`.
    */
  def from(value: String): Either[DomainError, Email] =
    val trimmed = value.trim
    if trimmed.isEmpty then Left(InvalidValue("email", "Email must not be empty"))
    else if !trimmed.contains("@") then Left(InvalidValue("email", s"Email must contain '@': '$value'"))
    else Right(trimmed)

  /** Непроверяющий конструктор (для использования в репозиториях/тестах). */
  def apply(value: String): Email = value

  /** Доступ к сырому строковому значению. */
  extension (email: Email) def value: String = email
