package inc.uberpopug.common.domain

import java.util.UUID

import scala.util.Try

import inc.uberpopug.common.domain.DomainError.InvalidValue

opaque type TaskId = UUID

object TaskId:
  def apply(value: UUID): TaskId = value

  def from(value: String): Either[DomainError, TaskId] =
    Try(UUID.fromString(value)).toEither.left
      .map(_ => InvalidValue("taskId", s"Invalid UUID: '$value'"))
      .map(TaskId(_))

  extension (taskId: TaskId) def value: UUID = taskId
