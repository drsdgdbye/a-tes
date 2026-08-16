package inc.uberpopug.taskservice.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Описание задачи: непустое. Опционально — отсутствие описания моделируется как `None`. */
opaque type TaskDescription = String

object TaskDescription:
  /** Smart-конструктор: обрезает пробелы и отвергает пустые значения. */
  def from(value: String): Either[DomainError, TaskDescription] =
    val trimmed = value.trim
    if trimmed.isEmpty then Left(InvalidValue("description", "Task description must not be empty"))
    else Right(trimmed)

  /** Непроверяющий конструктор (для использования в репозиториях/тестах). */
  def apply(value: String): TaskDescription = value

  /** Доступ к сырому строковому значению. */
  extension (description: TaskDescription) def value: String = description
