package inc.uberpopug.auth.repository

import java.sql.SQLException
import java.time.Instant
import java.util.UUID

import inc.uberpopug.auth.db.DbContext.Postgres
import inc.uberpopug.auth.domain.{Email, PasswordHash, Role, User, UserStatus}
import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.{EmailAlreadyExists, PersistenceError}
import inc.uberpopug.common.domain.UserId
import io.getquill.*
import zio.{ZIO, ZLayer}

/** Строка таблицы `users` в формате БД. */
final case class UserRow(
    id: UUID,
    name: String,
    email: String,
    passwordHash: String,
    role: String,
    status: String,
    version: Int,
    createdAt: Instant,
    updatedAt: Instant
)

object UserRow:
  /** Маппит доменного пользователя в строку БД (enum → wire-строка). */
  def fromUser(user: User): UserRow =
    UserRow(
      id = user.id.value,
      name = user.name,
      email = user.email.value,
      passwordHash = user.passwordHash.value,
      role = user.role.wire,
      status = user.status.wire,
      version = user.version,
      createdAt = user.createdAt,
      updatedAt = user.updatedAt
    )

  /** Восстанавливает доменного пользователя из строки БД; ошибка при невалидных полях. */
  def toUser(row: UserRow): Either[DomainError, User] =
    for
      role <- Role.from(row.role)
      status <- UserStatus.from(row.status)
    yield User(
      id = UserId(row.id),
      name = row.name,
      email = Email(row.email),
      passwordHash = PasswordHash(row.passwordHash),
      role = role,
      status = status,
      version = row.version,
      createdAt = row.createdAt,
      updatedAt = row.updatedAt
    )

/** Репозиторий пользователей. */
trait UserRepository:
  /** Создаёт пользователя и запись outbox в одной транзакции (атомарно). */
  def createWithOutbox(user: User, outbox: OutboxRecord): ZIO[Any, DomainError, Unit]

  /** Ищет пользователя по email (уникален). */
  def findByEmail(email: Email): ZIO[Any, DomainError, Option[User]]

  /** Ищет пользователя по id. */
  def findById(id: UserId): ZIO[Any, DomainError, Option[User]]

  /** Возвращает страницу пользователей, отсортированную по дате создания. */
  def list(limit: Int, offset: Int): ZIO[Any, DomainError, List[User]]

  /** Обновляет имя, роль, статус и `updatedAt` пользователя. */
  def update(user: User): ZIO[Any, DomainError, Unit]

  /** Обновляет хэш пароля, версию учётных данных и `updatedAt` пользователя. */
  def updatePassword(user: User): ZIO[Any, DomainError, Unit]

object UserRepository:
  /** Слой репозитория поверх Quill-контекста Postgres. */
  val layer: ZLayer[Postgres, Nothing, UserRepository] =
    ZLayer.fromFunction(UserRepositoryLive(_))

/** Quill-реализация репозитория пользователей поверх Postgres. */
final case class UserRepositoryLive(ctx: Postgres) extends UserRepository:
  import ctx.*
  import Tables.given

  /** Оборачивает SQL-ошибку в `PersistenceError`. */
  private def toPersistenceError(ex: Throwable): DomainError =
    ex match
      case e: SQLException if e.getSQLState == "23505" => PersistenceError(e.getMessage)
      case other => PersistenceError(Option(other.getMessage).getOrElse(other.getClass.getSimpleName))

  /** Оборачивает SQL-ошибку; нарушение unique-constraint (`23505`) — `EmailAlreadyExists`. */
  private def toDomainError(email: Email): Throwable => DomainError = {
    case e: SQLException if e.getSQLState == "23505" => EmailAlreadyExists(email.value)
    case other => PersistenceError(Option(other.getMessage).getOrElse(other.getClass.getSimpleName))
  }

  /** Парсит строку БД в доменного пользователя; повреждённая строка — ошибка. */
  private def parseRow(row: UserRow): ZIO[Any, DomainError, User] =
    ZIO.fromEither(UserRow.toUser(row)).mapError(_ => PersistenceError(s"Corrupted user row: ${row.id}"))

  /** Вставка пользователя и события в outbox в одной транзакции. */
  def createWithOutbox(user: User, outbox: OutboxRecord): ZIO[Any, DomainError, Unit] =
    ctx
      .transaction {
        run(query[UserRow].insertValue(lift(UserRow.fromUser(user)))) *>
          run(query[OutboxRow].insertValue(lift(OutboxRow.fromRecord(outbox))))
      }
      .unit
      .mapError(toDomainError(user.email))

  /** Ищет пользователя по email; отсутствие — `None`. */
  def findByEmail(email: Email): ZIO[Any, DomainError, Option[User]] =
    run(query[UserRow].filter(_.email == lift(email.value)))
      .map(_.headOption)
      .mapError(toPersistenceError)
      .flatMap {
        case Some(row) => parseRow(row).map(Some(_))
        case None      => ZIO.none
      }

  /** Ищет пользователя по id; отсутствие — `None`. */
  def findById(id: UserId): ZIO[Any, DomainError, Option[User]] =
    run(query[UserRow].filter(_.id == lift(id.value)))
      .map(_.headOption)
      .mapError(toPersistenceError)
      .flatMap {
        case Some(row) => parseRow(row).map(Some(_))
        case None      => ZIO.none
      }

  /** Возвращает страницу пользователей по `created_at` без учёта статуса. */
  def list(limit: Int, offset: Int): ZIO[Any, DomainError, List[User]] =
    run(
      query[UserRow]
        .sortBy(_.createdAt)(using Ord.asc)
        .drop(lift(offset))
        .take(lift(limit))
    )
      .mapError(toPersistenceError)
      .flatMap(rows => ZIO.foreach(rows)(parseRow))

  /** Обновляет изменяемые поля пользователя (имя, роль, статус, `updated_at`). */
  def update(user: User): ZIO[Any, DomainError, Unit] =
    run(
      query[UserRow]
        .filter(_.id == lift(user.id.value))
        .update(
          _.name -> lift(user.name),
          _.role -> lift(user.role.wire),
          _.status -> lift(user.status.wire),
          _.updatedAt -> lift(user.updatedAt)
        )
    ).unit
      .mapError(toPersistenceError)

  /** Обновляет хэш пароля, версию и `updatedAt` по id. */
  def updatePassword(user: User): ZIO[Any, DomainError, Unit] =
    run(
      query[UserRow]
        .filter(_.id == lift(user.id.value))
        .update(
          _.passwordHash -> lift(user.passwordHash.value),
          _.version -> lift(user.version),
          _.updatedAt -> lift(user.updatedAt)
        )
    ).unit
      .mapError(toPersistenceError)
