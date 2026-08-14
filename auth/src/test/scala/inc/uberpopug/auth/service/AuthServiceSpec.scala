package inc.uberpopug.auth.service

import java.security.KeyPairGenerator
import java.security.spec.ECGenParameterSpec
import java.time.Instant
import java.util.UUID

import zio.*
import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.auth.config.JwtConfig
import inc.uberpopug.auth.domain.*
import inc.uberpopug.auth.repository.*
import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.*
import inc.uberpopug.common.domain.UserId

/** Состояние in-memory репозитория пользователей: пользователи и записи outbox. */
final case class UserRepoState(users: Map[UUID, User] = Map.empty, outbox: Chunk[OutboxRecord] = Chunk.empty)

/** In-memory реализация UserRepository для тестов на Ref. */
final case class UserRepoInMemory(state: Ref[UserRepoState]) extends UserRepository:
  def createWithOutbox(user: User, outbox: OutboxRecord): ZIO[Any, DomainError, Unit] =
    state.update(s => s.copy(users = s.users + (user.id.value -> user), outbox = s.outbox :+ outbox))

  def findByEmail(email: Email): ZIO[Any, DomainError, Option[User]] =
    state.get.map(_.users.values.find(_.email == email))

  def findById(id: UserId): ZIO[Any, DomainError, Option[User]] =
    state.get.map(_.users.get(id.value))

  def list(limit: Int, offset: Int): ZIO[Any, DomainError, List[User]] =
    state.get.map(_.users.values.toList.sortBy(_.createdAt).slice(offset, offset + limit))

  def update(user: User): ZIO[Any, DomainError, Unit] =
    state.update(s => s.copy(users = s.users + (user.id.value -> user)))

/** In-memory реализация RefreshTokenRepository для тестов на Ref. */
final case class RefreshTokenRepoInMemory(state: Ref[Map[UUID, RefreshTokenRow]]) extends RefreshTokenRepository:
  def insert(row: RefreshTokenRow): ZIO[Any, DomainError, Unit] =
    state.update(_ + (row.id -> row))

  def findByHash(hash: RefreshTokenHash): ZIO[Any, DomainError, Option[RefreshTokenRow]] =
    state.get.map(_.values.find(_.hash == hash.value))

  def revoke(id: UUID): ZIO[Any, DomainError, Unit] =
    state.update(m => m.get(id).fold(m)(row => m + (id -> row.copy(revoked = true))))

  def revokeAllByUser(userId: UserId): ZIO[Any, DomainError, Unit] =
    state.update(_.map { case (id, row) => id -> row.copy(revoked = row.revoked || row.userId == userId.value) })

/** Тесты AuthService: логин, ротация refresh, создание/обновление/список пользователей. */
object AuthServiceSpec extends ZIOSpecDefault:
  /** Тестовый JWT-конфиг: короткий access-срок для проверки TTL. */
  private val cfg = JwtConfig(issuer = "ates-test", accessTtlSeconds = 900, refreshTtlSeconds = 604800)

  /** Фиксированный момент времени для детерминированных тестов. */
  private val now = Instant.parse("2026-01-01T00:00:00Z")

  /** Сервис токенов с тестовым эфемерным ключом EC P-256. */
  private def tokenService: TokenService =
    val generator = KeyPairGenerator.getInstance("EC")
    generator.initialize(new ECGenParameterSpec("secp256r1"))
    TokenServiceLive(cfg, generator.generateKeyPair())

  /** Собирает AuthServiceLive на in-memory репозиториях и возвращает их Ref'ы. */
  private def makeService: ZIO[Any, Nothing, (AuthService, Ref[UserRepoState], Ref[Map[UUID, RefreshTokenRow]])] =
    for
      usersRef <- Ref.make(UserRepoState())
      tokensRef <- Ref.make(Map.empty[UUID, RefreshTokenRow])
    yield (
      AuthServiceLive(
        UserRepoInMemory(usersRef),
        RefreshTokenRepoInMemory(tokensRef),
        PasswordHasherLive,
        tokenService
      ),
      usersRef,
      tokensRef
    )

  /** Добавляет пользователя в in-memory репозиторий с заданными ролью и статусом. */
  private def seedUser(
      usersRef: Ref[UserRepoState],
      name: String,
      email: String,
      password: String,
      role: Role,
      status: UserStatus = UserStatus.Active
  ): ZIO[Any, Nothing, User] =
    for
      hash <- PasswordHasherLive.hash(Password.from(password).toOption.get)
      id = UserId(UUID.randomUUID())
      user = User.create(id, name, Email(email), hash, role, now).toOption.get
      seeded = if status == UserStatus.Active then user else User.withStatus(user, status, now)
      _ <- usersRef.update(s => s.copy(users = s.users + (id.value -> seeded)))
    yield seeded

  /** Снижает env-требование эффекта с `Clock` до `Any`, передавая Live-реализацию Clock как слой. ZIO Test при этом всё
    * равно управляет временем через DefaultServices (TestClock), поэтому TTL-проверки остаются детерминированными.
    */
  private def withLiveClock[E, A](effect: ZIO[Clock, E, A]): ZIO[Any, E, A] =
    effect.provideLayer(ZLayer.succeed(Clock.ClockLive))

  def spec: Spec[Any, Any] =
    suite("AuthService")(
      suite("login")(
        test("succeeds with valid credentials") {
          withLiveClock {
            for
              tuple <- makeService
              (service, usersRef, _) = tuple
              _ <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              pair <- service.login("popug@ates.io", "secret123")
            yield assertTrue(pair.accessToken.value.nonEmpty, pair.refreshToken.value.nonEmpty)
          }
        },
        test("fails with an unknown email") {
          withLiveClock {
            for
              service <- makeService.map(_._1)
              result <- service.login("ghost@ates.io", "secret123").exit
            yield assert(result)(fails(equalTo(InvalidCredentials)))
          }
        },
        test("fails with a wrong password") {
          withLiveClock {
            for
              tuple <- makeService
              (service, usersRef, _) = tuple
              _ <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              result <- service.login("popug@ates.io", "wrong-password").exit
            yield assert(result)(fails(equalTo(InvalidCredentials)))
          }
        },
        test("fails for a disabled user") {
          withLiveClock {
            for
              tuple <- makeService
              (service, usersRef, _) = tuple
              _ <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug, UserStatus.Disabled)
              userId <- usersRef.get.map(_.users.values.head.id.value.toString)
              result <- service.login("popug@ates.io", "secret123").exit
            yield assert(result)(fails(equalTo(UserDisabled(userId))))
          }
        }
      ),
      suite("refresh")(
        test("rotates the refresh token and revokes the previous one") {
          withLiveClock {
            for
              tuple <- makeService
              (service, usersRef, _) = tuple
              _ <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              pair <- service.login("popug@ates.io", "secret123")
              first = pair.refreshToken.value
              rotated <- service.refresh(first)
              stale <- service.refresh(first).exit
            yield assertTrue(rotated.refreshToken.value != first, stale.isFailure)
          }
        },
        test("fails for an unknown refresh token") {
          withLiveClock {
            for
              service <- makeService.map(_._1)
              result <- service.refresh(UUID.randomUUID().toString).exit
            yield assert(result)(fails(isSubtype[RefreshTokenInvalid](anything)))
          }
        }
      ),
      suite("createUser")(
        test("denies non-admin actors") {
          withLiveClock {
            for
              tuple <- makeService
              (service, usersRef, _) = tuple
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              result <- service
                .createUser("New", "new@ates.io", "pass123", Role.Popug, AuthenticatedUser(popug.id, Role.Popug))
                .exit
            yield assert(result)(fails(equalTo(AccessDenied("Admin privileges required"))))
          }
        },
        test("creates a user and writes a UserCreated outbox record") {
          withLiveClock {
            for
              tuple <- makeService
              (service, usersRef, _) = tuple
              admin <- seedUser(usersRef, "Admin", "admin@ates.io", "admin-pass", Role.Admin)
              created <- service.createUser(
                "New Popug",
                "new@ates.io",
                "pass123",
                Role.Popug,
                AuthenticatedUser(admin.id, Role.Admin)
              )
              outbox <- usersRef.get.map(_.outbox)
            yield assertTrue(
              created.email.value == "new@ates.io",
              created.role == Role.Popug,
              outbox.size == 1,
              outbox.head.eventType == "UserCreated"
            )
          }
        },
        test("fails on a duplicate email") {
          withLiveClock {
            for
              tuple <- makeService
              (service, usersRef, _) = tuple
              admin <- seedUser(usersRef, "Admin", "admin@ates.io", "admin-pass", Role.Admin)
              _ <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              result <- service
                .createUser("Clone", "popug@ates.io", "pass123", Role.Popug, AuthenticatedUser(admin.id, Role.Admin))
                .exit
            yield assert(result)(fails(isSubtype[EmailAlreadyExists](anything)))
          }
        }
      ),
      suite("updateUser")(
        test("denies non-admin actors") {
          withLiveClock {
            for
              tuple <- makeService
              (service, usersRef, _) = tuple
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              result <- service
                .updateUser(popug.id, Some(Role.Manager), None, AuthenticatedUser(popug.id, Role.Popug))
                .exit
            yield assert(result)(fails(equalTo(AccessDenied("Admin privileges required"))))
          }
        },
        test("forbids an admin from disabling themselves") {
          withLiveClock {
            for
              tuple <- makeService
              (service, usersRef, _) = tuple
              admin <- seedUser(usersRef, "Admin", "admin@ates.io", "admin-pass", Role.Admin)
              result <- service
                .updateUser(admin.id, None, Some(UserStatus.Disabled), AuthenticatedUser(admin.id, Role.Admin))
                .exit
            yield assert(result)(fails(equalTo(SelfDisableForbidden)))
          }
        },
        test("admin disables another user and revokes their refresh tokens") {
          withLiveClock {
            for
              tuple <- makeService
              (service, usersRef, tokensRef) = tuple
              admin <- seedUser(usersRef, "Admin", "admin@ates.io", "admin-pass", Role.Admin)
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              _ <- service.login("popug@ates.io", "secret123")
              updated <- service.updateUser(
                popug.id,
                None,
                Some(UserStatus.Disabled),
                AuthenticatedUser(admin.id, Role.Admin)
              )
              rows <- tokensRef.get
            yield assertTrue(
              updated.status == UserStatus.Disabled,
              rows.values.exists(row => row.userId == popug.id.value && row.revoked)
            )
          }
        }
      ),
      suite("listUsers")(
        test("denies non-admin actors") {
          for
            tuple <- makeService
            (service, usersRef, _) = tuple
            popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
            result <- service.listUsers(10, 0, AuthenticatedUser(popug.id, Role.Popug)).exit
          yield assert(result)(fails(equalTo(AccessDenied("Admin privileges required"))))
        }
      )
    )
