package inc.uberpopug.auth.service

import java.security.KeyPairGenerator
import java.security.spec.ECGenParameterSpec
import java.time.Instant
import java.util.UUID

import zio.*
import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.auth.config.{AuthConfig, JwtConfig}
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

  def updatePassword(user: User): ZIO[Any, DomainError, Unit] =
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
  private def makeService(
      authConfig: AuthConfig = AuthConfig(registrationEnabled = true)
  ): ZIO[Any, Nothing, (AuthService, Ref[UserRepoState], Ref[Map[UUID, RefreshTokenRow]])] =
    for
      usersRef <- Ref.make(UserRepoState())
      tokensRef <- Ref.make(Map.empty[UUID, RefreshTokenRow])
    yield (
      AuthServiceLive(
        UserRepoInMemory(usersRef),
        RefreshTokenRepoInMemory(tokensRef),
        PasswordHasherLive,
        tokenService,
        authConfig
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
              tuple <- makeService()
              (service, usersRef, _) = tuple
              _ <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              pair <- service.login("popug@ates.io", "secret123")
            yield assertTrue(pair.accessToken.value.nonEmpty, pair.refreshToken.value.nonEmpty)
          }
        },
        test("fails with an unknown email") {
          withLiveClock {
            for
              service <- makeService().map(_._1)
              result <- service.login("ghost@ates.io", "secret123").exit
            yield assert(result)(fails(equalTo(InvalidCredentials)))
          }
        },
        test("fails with a wrong password") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              _ <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              result <- service.login("popug@ates.io", "wrong-password").exit
            yield assert(result)(fails(equalTo(InvalidCredentials)))
          }
        },
        test("fails for a disabled user") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              _ <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug, UserStatus.Disabled)
              userId <- usersRef.get.map(_.users.values.head.id.value.toString)
              result <- service.login("popug@ates.io", "secret123").exit
            yield assert(result)(fails(equalTo(UserDisabled(userId))))
          }
        }
      ),
      suite("register")(
        test("registers a popug with auto-login and writes a UserCreated outbox record") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              pair <- service.register("New Popug", "new@ates.io", "pass123")
              user <- usersRef.get.map(_.users.values.find(_.email.value == "new@ates.io"))
              login <- service.login("new@ates.io", "pass123").exit
              outbox <- usersRef.get.map(_.outbox)
            yield assertTrue(
              user.exists(_.role == Role.Popug),
              user.exists(_.status == UserStatus.Active),
              pair.accessToken.value.nonEmpty,
              pair.refreshToken.value.nonEmpty,
              login.isSuccess,
              outbox.size == 1,
              outbox.head.eventType == "UserCreated"
            )
          }
        },
        test("fails on a duplicate email") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              _ <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              result <- service.register("Clone", "popug@ates.io", "pass123").exit
            yield assert(result)(fails(isSubtype[EmailAlreadyExists](anything)))
          }
        },
        test("fails when registration is disabled") {
          withLiveClock {
            for
              tuple <- makeService(AuthConfig(registrationEnabled = false))
              (service, _, _) = tuple
              result <- service.register("New Popug", "new@ates.io", "pass123").exit
            yield assert(result)(fails(equalTo(RegistrationDisabled)))
          }
        }
      ),
      suite("refresh")(
        test("rotates the refresh token and revokes the previous one") {
          withLiveClock {
            for
              tuple <- makeService()
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
              service <- makeService().map(_._1)
              result <- service.refresh(UUID.randomUUID().toString).exit
            yield assert(result)(fails(isSubtype[RefreshTokenInvalid](anything)))
          }
        }
      ),
      suite("createUser")(
        test("denies non-admin actors") {
          withLiveClock {
            for
              tuple <- makeService()
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
              tuple <- makeService()
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
              tuple <- makeService()
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
              tuple <- makeService()
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
              tuple <- makeService()
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
              tuple <- makeService()
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
            tuple <- makeService()
            (service, usersRef, _) = tuple
            popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
            result <- service.listUsers(10, 0, AuthenticatedUser(popug.id, Role.Popug)).exit
          yield assert(result)(fails(equalTo(AccessDenied("Admin privileges required"))))
        }
      ),
      suite("changePassword")(
        test("changes own password, bumps version and invalidates the previous refresh token") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              pair <- service.login("popug@ates.io", "secret123")
              _ <- service.changePassword("secret123", "new-secret456", AuthenticatedUser(popug.id, Role.Popug))
              newLogin <- service.login("popug@ates.io", "new-secret456").exit
              oldLogin <- service.login("popug@ates.io", "secret123").exit
              staleRefresh <- service.refresh(pair.refreshToken.value).exit
            yield assertTrue(
              newLogin.isSuccess
            ) && assert(oldLogin)(fails(equalTo(InvalidCredentials))) && assert(staleRefresh)(
              fails(isSubtype[RefreshTokenInvalid](anything))
            )
          }
        },
        test("fails with a wrong current password") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              result <- service
                .changePassword("wrong-password", "new-secret456", AuthenticatedUser(popug.id, Role.Popug))
                .exit
            yield assert(result)(fails(equalTo(InvalidCredentials)))
          }
        },
        test("increments the stored credential version (M-AUTH-01)") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              _ <- service.changePassword("secret123", "new-secret456", AuthenticatedUser(popug.id, Role.Popug))
              stored <- usersRef.get.map(_.users(popug.id.value))
            yield assertTrue(stored.version == 1)
          }
        },
        test("a refresh token issued after the change still works (M-AUTH-03)") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              _ <- service.login("popug@ates.io", "secret123")
              _ <- service.changePassword("secret123", "new-secret456", AuthenticatedUser(popug.id, Role.Popug))
              newPair <- service.login("popug@ates.io", "new-secret456")
              rotated <- service.refresh(newPair.refreshToken.value).exit
            yield assertTrue(rotated.isSuccess)
          }
        },
        test("after a double password change both older pairs are rejected (M-AUTH-05)") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              firstPair <- service.login("popug@ates.io", "secret123")
              _ <- service.changePassword("secret123", "new-secret456", AuthenticatedUser(popug.id, Role.Popug))
              secondPair <- service.login("popug@ates.io", "new-secret456")
              _ <- service.changePassword("new-secret456", "final-secret789", AuthenticatedUser(popug.id, Role.Popug))
              firstStale <- service.refresh(firstPair.refreshToken.value).exit
              secondStale <- service.refresh(secondPair.refreshToken.value).exit
              thirdLogin <- service.login("popug@ates.io", "final-secret789").exit
            yield assertTrue(
              firstStale.isFailure,
              secondStale.isFailure,
              thirdLogin.isSuccess
            )
          }
        }
      ),
      suite("resetPassword")(
        test("denies non-admin actors") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              result <- service
                .resetPassword(popug.id, "new-secret456", AuthenticatedUser(popug.id, Role.Popug))
                .exit
            yield assert(result)(fails(equalTo(AccessDenied("Admin privileges required"))))
          }
        },
        test("resets the password, bumps version and invalidates old credentials and refresh tokens") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              admin <- seedUser(usersRef, "Admin", "admin@ates.io", "admin-pass", Role.Admin)
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              pair <- service.login("popug@ates.io", "secret123")
              _ <- service.resetPassword(popug.id, "reset-secret789", AuthenticatedUser(admin.id, Role.Admin))
              newLogin <- service.login("popug@ates.io", "reset-secret789").exit
              oldLogin <- service.login("popug@ates.io", "secret123").exit
              staleRefresh <- service.refresh(pair.refreshToken.value).exit
            yield assertTrue(
              newLogin.isSuccess
            ) && assert(oldLogin)(fails(equalTo(InvalidCredentials))) && assert(staleRefresh)(
              fails(isSubtype[RefreshTokenInvalid](anything))
            )
          }
        },
        test("admin can reset their own password (M-AUTH-08)") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              admin <- seedUser(usersRef, "Admin", "admin@ates.io", "admin-pass", Role.Admin)
              _ <- service.resetPassword(admin.id, "new-admin-pass", AuthenticatedUser(admin.id, Role.Admin))
              stored <- usersRef.get.map(_.users(admin.id.value))
              login <- service.login("admin@ates.io", "new-admin-pass").exit
            yield assertTrue(stored.version == 1, stored.name == "Admin", login.isSuccess)
          }
        }
      ),
      suite("logout")(
        test("is idempotent for an already-revoked token (M-AUTH-22)") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              _ <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              pair <- service.login("popug@ates.io", "secret123")
              _ <- service.logout(pair.refreshToken.value)
              second <- service.logout(pair.refreshToken.value).exit
            yield assertTrue(second.isSuccess)
          }
        },
        test("fails for an unknown token (M-AUTH-22 current contract)") {
          withLiveClock {
            for
              service <- makeService().map(_._1)
              result <- service.logout(UUID.randomUUID().toString).exit
            yield assert(result)(fails(isSubtype[RefreshTokenInvalid](anything)))
          }
        }
      ),
      suite("refresh credential expiry")(
        test("fails when the refresh token has expired by TTL (M-AUTH-20)") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, tokensRef) = tuple
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              token <- ZIO.succeed(RefreshToken.from(UUID.randomUUID().toString).toOption.get)
              hash = RefreshTokenHash.of(token)
              _ <- tokensRef.update(
                _ + (UUID.randomUUID() -> RefreshTokenRow(
                  id = UUID.randomUUID(),
                  userId = popug.id.value,
                  hash = hash.value,
                  version = popug.version,
                  expiresAt = Instant.EPOCH.minusSeconds(1),
                  revoked = false,
                  createdAt = Instant.EPOCH
                ))
              )
              result <- service.refresh(token.value).exit
            yield assert(result)(fails(isSubtype[RefreshTokenInvalid](anything)))
          }
        },
        test("refresh fails after the user is disabled (M-AUTH-11)") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              admin <- seedUser(usersRef, "Admin", "admin@ates.io", "admin-pass", Role.Admin)
              popug <- seedUser(usersRef, "Popug", "popug@ates.io", "secret123", Role.Popug)
              pair <- service.login("popug@ates.io", "secret123")
              _ <- service.updateUser(
                popug.id,
                None,
                Some(UserStatus.Disabled),
                AuthenticatedUser(admin.id, Role.Admin)
              )
              result <- service.refresh(pair.refreshToken.value).exit
            yield assert(result)(fails(isSubtype[RefreshTokenInvalid](anything)))
          }
        }
      ),
      suite("outbox")(
        test("createUser outbox record references the created user (M-AUTH-28)") {
          withLiveClock {
            for
              tuple <- makeService()
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
              record = outbox.head
              event = auth.user_created.UserCreated.parseFrom(record.payload)
            yield assertTrue(
              record.aggregateId == created.id.value,
              record.eventType == "UserCreated",
              event.userId == created.id.value.toString
            )
          }
        },
        test("register outbox record references the created user (M-AUTH-28)") {
          withLiveClock {
            for
              tuple <- makeService()
              (service, usersRef, _) = tuple
              _ <- service.register("New Popug", "new@ates.io", "pass123")
              user <- usersRef.get.map(_.users.values.find(_.email.value == "new@ates.io").get)
              record <- usersRef.get.map(_.outbox.head)
              event = auth.user_created.UserCreated.parseFrom(record.payload)
            yield assertTrue(
              record.aggregateId == user.id.value,
              event.userId == user.id.value.toString
            )
          }
        }
      )
    )
