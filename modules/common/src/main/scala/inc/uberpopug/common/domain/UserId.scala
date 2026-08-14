package inc.uberpopug.common.domain

import java.util.UUID

import scala.util.Try

import inc.uberpopug.common.domain.DomainError.InvalidValue

opaque type UserId = UUID

object UserId:
  def apply(value: UUID): UserId = value

  def from(value: String): Either[DomainError, UserId] =
    Try(UUID.fromString(value)).toEither.left
      .map(_ => InvalidValue("userId", s"Invalid UUID: '$value'"))
      .map(UserId(_))

  extension (userId: UserId) def value: UUID = userId
