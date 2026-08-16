package inc.uberpopug.taskservice.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Заголовок задачи: непустой, не длиннее 500 символов. */
opaque type TaskTitle = String

object TaskTitle:
  /** Максимальная длина заголовка в символах. */
  val MaxLength = 500

  /** Smart-конструктор: обрезает пробелы и проверяет непустоту и длину. */
  def from(value: String): Either[DomainError, TaskTitle] =
    val trimmed = value.trim
    if trimmed.isEmpty then Left(InvalidValue("title", "Task title must not be empty"))
    else if trimmed.length > MaxLength then
      Left(InvalidValue("title", s"Task title must not exceed $MaxLength characters"))
    else Right(trimmed)

  /** Непроверяющий конструктор (для использования в репозиториях/тестах). */
  def apply(value: String): TaskTitle = value

  /** Доступ к сырому строковому значению. */
  extension (title: TaskTitle) def value: String = title
