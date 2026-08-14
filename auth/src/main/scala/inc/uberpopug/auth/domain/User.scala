package inc.uberpopug.auth.domain

import java.time.Instant

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue
import inc.uberpopug.common.domain.UserId

/** Пользователь — корневая сущность Auth Service (aggregate root). */
final case class User(
    id: UserId,
    name: String,
    email: Email,
    passwordHash: PasswordHash,
    role: Role,
    status: UserStatus,
    createdAt: Instant,
    updatedAt: Instant
)

object User:
  /** Создаёт пользователя: валидирует имя и проставляет начальные значения — статус `Active` и одинаковые
    * `createdAt`/`updatedAt`.
    */
  def create(
      id: UserId,
      name: String,
      email: Email,
      passwordHash: PasswordHash,
      role: Role,
      now: Instant
  ): Either[DomainError, User] =
    validateName(name).map { n =>
      User(id, n, email, passwordHash, role, UserStatus.Active, now, now)
    }

  /** Возвращает копию пользователя с новой ролью и обновлённым `updatedAt`. */
  def withRole(user: User, role: Role, now: Instant): User =
    user.copy(role = role, updatedAt = now)

  /** Возвращает копию пользователя с новым статусом и обновлённым `updatedAt`. */
  def withStatus(user: User, status: UserStatus, now: Instant): User =
    user.copy(status = status, updatedAt = now)

  /** Валидирует имя: непустое после trim и не длиннее 255 символов. Возвращает каноническое (обрезанное) значение.
    */
  private def validateName(name: String): Either[DomainError, String] =
    val trimmed = name.trim
    if trimmed.isEmpty then Left(InvalidValue("name", "Name must not be empty"))
    else if trimmed.length > 255 then Left(InvalidValue("name", "Name must not exceed 255 characters"))
    else Right(trimmed)
