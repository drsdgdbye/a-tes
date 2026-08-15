package inc.uberpopug.auth.service

import java.time.Instant
import java.util.UUID

import zio.{Clock, ZIO, ZLayer}

import inc.uberpopug.auth.domain.*
import inc.uberpopug.auth.config.AuthConfig
import inc.uberpopug.auth.repository.*
import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.*
import inc.uberpopug.common.domain.UserId

/** Результат аутентификации: пара access/refresh токенов и срок действия refresh. */
final case class TokenPair(
    accessToken: AccessToken,
    refreshToken: RefreshToken,
    refreshExpiresAt: Instant
)

/** Use case'ы аутентификации и управления пользователями. */
trait AuthService:
  /** Аутентификация по логину/паролю: выдача пары токенов. */
  def login(login: String, password: String): ZIO[Clock, DomainError, TokenPair]

  /** Саморегистрация пользователя с ролью `Popug`: создание + auto-login. Отключена конфиг-флагом. */
  def register(name: String, email: String, password: String): ZIO[Clock, DomainError, TokenPair]

  /** Ротация refresh-токена: выдача новой пары и отзыв старого токена. */
  def refresh(refreshToken: String): ZIO[Clock, DomainError, TokenPair]

  /** Завершение сессии: отзыв refresh-токена. */
  def logout(refreshToken: String): ZIO[Any, DomainError, Unit]

  /** Смена собственного пароля: проверка старого пароля, инкремент версии, отзыв всех refresh-токенов. */
  def changePassword(
      currentPassword: String,
      newPassword: String,
      actor: AuthenticatedUser
  ): ZIO[Clock, DomainError, Unit]

  /** Сброс пароля пользователя админом: без проверки старого, инкремент версии, отзыв всех refresh-токенов. */
  def resetPassword(id: UserId, newPassword: String, actor: AuthenticatedUser): ZIO[Clock, DomainError, Unit]

  /** Создание пользователя (admin): валидация + транзакция user + outbox. */
  def createUser(
      name: String,
      email: String,
      password: String,
      role: Role,
      actor: AuthenticatedUser
  ): ZIO[Clock, DomainError, User]

  /** Получение пользователя по id (любой аутентифицированный). */
  def getUser(id: UserId, actor: AuthenticatedUser): ZIO[Any, DomainError, User]

  /** Список пользователей с пагинацией (admin). */
  def listUsers(limit: Int, offset: Int, actor: AuthenticatedUser): ZIO[Any, DomainError, List[User]]

  /** Обновление роли/статуса (admin); disable отзывает refresh-токены. */
  def updateUser(
      id: UserId,
      role: Option[Role],
      status: Option[UserStatus],
      actor: AuthenticatedUser
  ): ZIO[Clock, DomainError, User]

object AuthService:
  /** Слой сервиса: собирает зависимости репозиториев, hasher, TokenService, конфиг аутентификации и Clock. */
  val layer: ZLayer[
    UserRepository & RefreshTokenRepository & PasswordHasher & TokenService & AuthConfig & Clock,
    Nothing,
    AuthService
  ] =
    ZLayer.fromFunction(AuthServiceLive(_, _, _, _, _))

/** Реализация AuthService: оркестрирует репозитории и доменные правила. */
final case class AuthServiceLive(
    users: UserRepository,
    refreshTokens: RefreshTokenRepository,
    passwordHasher: PasswordHasher,
    tokenService: TokenService,
    authConfig: AuthConfig
) extends AuthService:

  /** Логин: валидирует email/пароль, сверяет bcrypt-хэш, проверяет активность и выдаёт пару токенов. Ошибки кредов не
    * раскрывают, что именно неверно.
    */
  def login(login: String, password: String): ZIO[Clock, DomainError, TokenPair] =
    for
      email <- ZIO.fromEither(Email.from(login)).mapError(_ => InvalidCredentials)
      user <- users.findByEmail(email).flatMap {
        case Some(user) => ZIO.succeed(user)
        case None       => ZIO.fail(InvalidCredentials)
      }
      pass <- ZIO.fromEither(Password.from(password)).mapError(_ => InvalidCredentials)
      ok <- passwordHasher.verify(pass, user.passwordHash)
      _ <- if !ok then ZIO.fail(InvalidCredentials) else ZIO.unit
      _ <- ensureActive(user)
      pair <- issuePair(user)
    yield pair

  /** Саморегистрация: роль всегда `Popug` (admin уточняет её через `updateUser`), создание пользователя и outbox-записи
    * в одной транзакции, затем auto-login — выдача пары токенов.
    */
  def register(name: String, email: String, password: String): ZIO[Clock, DomainError, TokenPair] =
    for
      _ <- if authConfig.registrationEnabled then ZIO.unit else ZIO.fail(RegistrationDisabled)
      email <- ZIO.fromEither(Email.from(email))
      _ <- users.findByEmail(email).flatMap {
        case Some(_) => ZIO.fail(EmailAlreadyExists(email.value))
        case None    => ZIO.unit
      }
      pass <- ZIO.fromEither(Password.from(password))
      hash <- passwordHasher.hash(pass)
      now <- Clock.instant
      id = UserId(UUID.randomUUID())
      user <- ZIO.fromEither(User.create(id, name, email, hash, Role.Popug, now))
      _ <- users.createWithOutbox(user, UserCreatedEvent.of(user, now))
      pair <- issuePair(user)
    yield pair

  /** Ротация refresh-токена: проверяет хэш, статус, срок действия и активность пользователя, выдаёт новую пару и
    * отзывает старый токен.
    */
  def refresh(refreshToken: String): ZIO[Clock, DomainError, TokenPair] =
    for
      token <- ZIO
        .fromEither(RefreshToken.from(refreshToken))
        .mapError(_ => RefreshTokenInvalid("Malformed refresh token"))
      hash = RefreshTokenHash.of(token)
      row <- refreshTokens.findByHash(hash).flatMap {
        case Some(row) => ZIO.succeed(row)
        case None      => ZIO.fail(RefreshTokenInvalid("Unknown refresh token"))
      }
      now <- Clock.instant
      _ <- if row.revoked then ZIO.fail(RefreshTokenInvalid("Refresh token has been revoked")) else ZIO.unit
      _ <- if row.expiresAt.isBefore(now) then ZIO.fail(RefreshTokenInvalid("Refresh token has expired")) else ZIO.unit
      userId = UserId(row.userId)
      user <- users.findById(userId).flatMap {
        case Some(user) => ZIO.succeed(user)
        case None       => ZIO.fail(RefreshTokenInvalid("User no longer exists"))
      }
      _ <-
        if user.version == row.version then ZIO.unit
        else ZIO.fail(RefreshTokenInvalid("Token version mismatch"))
      _ <- ensureActive(user)
      pair <- issuePair(user)
      _ <- refreshTokens.revoke(row.id)
    yield pair

  /** Logout: отзывает refresh-токен, если он существует и не отозван ранее. */
  def logout(refreshToken: String): ZIO[Any, DomainError, Unit] =
    for
      token <- ZIO
        .fromEither(RefreshToken.from(refreshToken))
        .mapError(_ => RefreshTokenInvalid("Malformed refresh token"))
      row <- refreshTokens.findByHash(RefreshTokenHash.of(token)).flatMap {
        case Some(row) => ZIO.succeed(row)
        case None      => ZIO.fail(RefreshTokenInvalid("Unknown refresh token"))
      }
      _ <- refreshTokens.revoke(row.id)
    yield ()

  /** Смена собственного пароля: находит пользователя по действующему лицу, проверяет старый пароль, инкрементирует
    * версию учётных данных и отзывает все refresh-токены.
    */
  def changePassword(
      currentPassword: String,
      newPassword: String,
      actor: AuthenticatedUser
  ): ZIO[Clock, DomainError, Unit] =
    for
      now <- Clock.instant
      user <- users.findById(actor.id).flatMap {
        case Some(user) => ZIO.succeed(user)
        case None       => ZIO.fail(UserNotFound(actor.id.value.toString))
      }
      current <- ZIO.fromEither(Password.from(currentPassword)).mapError(_ => InvalidCredentials)
      ok <- passwordHasher.verify(current, user.passwordHash)
      _ <- if !ok then ZIO.fail(InvalidCredentials) else ZIO.unit
      next <- ZIO
        .fromEither(Password.from(newPassword))
        .mapError(_ => InvalidValue("password", "Password must not be empty"))
      hash <- passwordHasher.hash(next)
      _ <- users.updatePassword(User.withPassword(user, hash, now))
      _ <- refreshTokens.revokeAllByUser(actor.id)
    yield ()

  /** Сброс пароля админом: без проверки старого пароля, инкремент версии и отзыв всех refresh-токенов пользователя. */
  def resetPassword(id: UserId, newPassword: String, actor: AuthenticatedUser): ZIO[Clock, DomainError, Unit] =
    for
      _ <- requireAdmin(actor)
      now <- Clock.instant
      user <- users.findById(id).flatMap {
        case Some(user) => ZIO.succeed(user)
        case None       => ZIO.fail(UserNotFound(id.value.toString))
      }
      next <- ZIO.fromEither(Password.from(newPassword))
      hash <- passwordHasher.hash(next)
      _ <- users.updatePassword(User.withPassword(user, hash, now))
      _ <- refreshTokens.revokeAllByUser(id)
    yield ()

  /** Создание пользователя (admin): проверка прав, валидация email/пароля/имени, уникальность email, хэширование пароля
    * и атомарная запись user + outbox.
    */
  def createUser(
      name: String,
      email: String,
      password: String,
      role: Role,
      actor: AuthenticatedUser
  ): ZIO[Clock, DomainError, User] =
    for
      _ <- requireAdmin(actor)
      email <- ZIO.fromEither(Email.from(email))
      _ <- users.findByEmail(email).flatMap {
        case Some(_) => ZIO.fail(EmailAlreadyExists(email.value))
        case None    => ZIO.unit
      }
      pass <- ZIO.fromEither(Password.from(password))
      hash <- passwordHasher.hash(pass)
      now <- Clock.instant
      id = UserId(UUID.randomUUID())
      user <- ZIO.fromEither(User.create(id, name, email, hash, role, now))
      _ <- users.createWithOutbox(user, UserCreatedEvent.of(user, now))
    yield user

  /** Получение пользователя по id; отсутствие — `UserNotFound`. */
  def getUser(id: UserId, actor: AuthenticatedUser): ZIO[Any, DomainError, User] =
    users.findById(id).flatMap {
      case Some(user) => ZIO.succeed(user)
      case None       => ZIO.fail(UserNotFound(id.value.toString))
    }

  /** Список пользователей с пагинацией; только admin. */
  def listUsers(limit: Int, offset: Int, actor: AuthenticatedUser): ZIO[Any, DomainError, List[User]] =
    requireAdmin(actor) *> users.list(limit, offset)

  /** Обновление роли/статуса (admin): запрещён self-disable, при переводе активного пользователя в Disabled отзываются
    * все его refresh-токены.
    */
  def updateUser(
      id: UserId,
      role: Option[Role],
      status: Option[UserStatus],
      actor: AuthenticatedUser
  ): ZIO[Clock, DomainError, User] =
    for
      _ <- requireAdmin(actor)
      now <- Clock.instant
      user <- users.findById(id).flatMap {
        case Some(user) => ZIO.succeed(user)
        case None       => ZIO.fail(UserNotFound(id.value.toString))
      }
      _ <- status match
        case Some(UserStatus.Disabled) if user.id == actor.id => ZIO.fail(SelfDisableForbidden)
        case _                                                => ZIO.unit
      withRole = role.fold(user)(r => User.withRole(user, r, now))
      withStatus = status.fold(withRole)(s => User.withStatus(withRole, s, now))
      _ <- if withStatus != user then users.update(withStatus) else ZIO.unit
      _ <-
        if user.status == UserStatus.Active && withStatus.status == UserStatus.Disabled then
          refreshTokens.revokeAllByUser(id)
        else ZIO.unit
    yield withStatus

  /** Выпускает пару токенов и сохраняет refresh-токен в БД (хранится хэш). */
  private def issuePair(user: User): ZIO[Clock, DomainError, TokenPair] =
    for
      now <- Clock.instant
      access <- tokenService.issueAccess(user.id, user.role)
      refresh <- tokenService.issueRefresh(user.id)
      _ <- refreshTokens.insert(
        RefreshTokenRow(
          id = UUID.randomUUID(),
          userId = user.id.value,
          hash = RefreshTokenHash.of(refresh.token).value,
          version = user.version,
          expiresAt = refresh.expiresAt,
          revoked = false,
          createdAt = now
        )
      )
    yield TokenPair(access, refresh.token, refresh.expiresAt)

  /** Проверяет, что действующее лицо — администратор. */
  private def requireAdmin(actor: AuthenticatedUser): ZIO[Any, DomainError, Unit] =
    if actor.role == Role.Admin then ZIO.unit
    else ZIO.fail(AccessDenied("Admin privileges required"))

  /** Проверяет, что пользователь активен; иначе — `UserDisabled`. */
  private def ensureActive(user: User): ZIO[Any, DomainError, Unit] =
    if user.status == UserStatus.Active then ZIO.unit
    else ZIO.fail(UserDisabled(user.id.value.toString))
