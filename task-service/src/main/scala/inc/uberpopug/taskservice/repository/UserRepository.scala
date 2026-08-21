package inc.uberpopug.taskservice.repository

import java.sql.SQLException
import java.util.UUID

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.PersistenceError
import inc.uberpopug.common.domain.UserId
import inc.uberpopug.taskservice.db.DbContext.Postgres
import inc.uberpopug.taskservice.domain.Role
import io.getquill.*
import zio.{ZIO, ZLayer}

/** Строка таблицы `users` в формате БД: проекция пользователей из Auth Service. */
final case class UserRow(
    userId: UUID,
    name: String,
    role: String
)

/** Репозиторий пользователей: хранит проекцию `UserCreated` для кэша кандидатов на ассайн. */
trait UserRepository:
  /** Вставляет пользователя, если его ещё нет (идемпотентно). */
  def insertIfAbsent(userId: UserId, name: String, role: Role): ZIO[Any, DomainError, Unit]

  /** Возвращает всех пользователей с ролью `popug`. */
  def findAllPopugs(): ZIO[Any, DomainError, List[UserId]]

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
      case e: SQLException => PersistenceError(Option(e.getMessage).getOrElse(e.getClass.getSimpleName))
      case other           => PersistenceError(Option(other.getMessage).getOrElse(other.getClass.getSimpleName))

  /** Вставляет пользователя; нарушение уникальности PK (`23505`) — уже существует. */
  def insertIfAbsent(userId: UserId, name: String, role: Role): ZIO[Any, DomainError, Unit] =
    run(
      query[UserRow]
        .insert(_.userId -> lift(userId.value), _.name -> lift(name), _.role -> lift(role.wire))
        .onConflictIgnore
    ).unit
      .mapError(toPersistenceError)

  /** Возвращает userId всех пользователей с ролью `popug`. */
  def findAllPopugs(): ZIO[Any, DomainError, List[UserId]] =
    run(query[UserRow].filter(_.role == lift(Role.Popug.wire)).map(_.userId))
      .map(_.map(UserId(_)))
      .mapError(toPersistenceError)
