package inc.uberpopug.auth.repository

import java.time.Instant
import java.util.UUID

import io.getquill.*

import inc.uberpopug.auth.db.DbContext.Postgres
import inc.uberpopug.auth.domain.RefreshTokenHash
import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.PersistenceError
import inc.uberpopug.common.domain.UserId
import zio.{ZIO, ZLayer}

/** Строка таблицы `refresh_tokens` в формате БД (хранится хэш, не сам токен). */
final case class RefreshTokenRow(
    id: UUID,
    userId: UUID,
    hash: String,
    version: Int,
    expiresAt: Instant,
    revoked: Boolean,
    createdAt: Instant
)

/** Репозиторий issued refresh-токенов. */
trait RefreshTokenRepository:
  /** Сохраняет новую строку refresh-токена. */
  def insert(row: RefreshTokenRow): ZIO[Any, DomainError, Unit]

  /** Ищет строку по SHA-256 хэшу токена. */
  def findByHash(hash: RefreshTokenHash): ZIO[Any, DomainError, Option[RefreshTokenRow]]

  /** Отзывает конкретный токен по id. */
  def revoke(id: UUID): ZIO[Any, DomainError, Unit]

  /** Отзывает все активные токены пользователя (например, при disable). */
  def revokeAllByUser(userId: UserId): ZIO[Any, DomainError, Unit]

object RefreshTokenRepository:
  /** Слой репозитория поверх Quill-контекста Postgres. */
  val layer: ZLayer[Postgres, Nothing, RefreshTokenRepository] =
    ZLayer.fromFunction(RefreshTokenRepositoryLive(_))

/** Quill-реализация репозитория refresh-токенов поверх Postgres. */
final case class RefreshTokenRepositoryLive(ctx: Postgres) extends RefreshTokenRepository:
  import ctx.*

  /** Оборачивает SQL-ошибку в `PersistenceError`. */
  private def toPersistenceError(ex: Throwable): DomainError =
    PersistenceError(Option(ex.getMessage).getOrElse(ex.getClass.getSimpleName))

  /** Вставляет новую строку refresh-токена. */
  def insert(row: RefreshTokenRow): ZIO[Any, DomainError, Unit] =
    run(query[RefreshTokenRow].insertValue(lift(row))).unit
      .mapError(toPersistenceError)

  /** Ищет строку по SHA-256 хэшу токена. */
  def findByHash(hash: RefreshTokenHash): ZIO[Any, DomainError, Option[RefreshTokenRow]] =
    run(query[RefreshTokenRow].filter(_.hash == lift(hash.value)))
      .map(_.headOption)
      .mapError(toPersistenceError)

  /** Отзывает токен: выставляет `revoked = true`. */
  def revoke(id: UUID): ZIO[Any, DomainError, Unit] =
    run(
      query[RefreshTokenRow]
        .filter(_.id == lift(id))
        .update(_.revoked -> true)
    ).unit
      .mapError(toPersistenceError)

  /** Отзывает все токены пользователя (выставление `revoked = true`). */
  def revokeAllByUser(userId: UserId): ZIO[Any, DomainError, Unit] =
    run(
      query[RefreshTokenRow]
        .filter(_.userId == lift(userId.value))
        .update(_.revoked -> true)
    ).unit
      .mapError(toPersistenceError)
