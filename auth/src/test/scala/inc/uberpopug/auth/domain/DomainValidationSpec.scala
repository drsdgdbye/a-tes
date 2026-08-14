package inc.uberpopug.auth.domain

import java.time.Instant
import java.util.UUID

import zio.test.Assertion.*
import zio.test.*

import inc.uberpopug.common.domain.DomainError.InvalidValue
import inc.uberpopug.common.domain.DomainError.{RefreshTokenInvalid, TokenInvalid}
import inc.uberpopug.common.domain.UserId

/** Тесты доменной валидации: email, роль, статус, пароль, хэш, токены, User. */
object DomainValidationSpec extends ZIOSpecDefault:
  def spec: Spec[Any, Any] =
    suite("Domain validation")(
      suite("Email.from")(
        test("accepts a valid email and trims whitespace") {
          assertTrue(Email.from("  popug@ates.io  ") == Right(Email("popug@ates.io")))
        },
        test("rejects an empty email") {
          assert(Email.from("  "))(isLeft(equalTo(InvalidValue("email", "Email must not be empty"))))
        },
        test("rejects an email without '@'") {
          assert(Email.from("popug"))(isLeft(equalTo(InvalidValue("email", "Email must contain '@': 'popug'"))))
        }
      ),
      suite("Role.from")(
        test("parses known roles case-insensitively") {
          assertTrue(
            Role.from("Popug") == Right(Role.Popug),
            Role.from("admin") == Right(Role.Admin),
            Role.from("accountant") == Right(Role.Accountant),
            Role.from("manager") == Right(Role.Manager)
          )
        },
        test("rejects unknown roles") {
          assert(Role.from("ceo"))(isLeft(isSubtype[InvalidValue](anything)))
        }
      ),
      suite("UserStatus.from")(
        test("parses known statuses") {
          assertTrue(
            UserStatus.from("active") == Right(UserStatus.Active),
            UserStatus.from("disabled") == Right(UserStatus.Disabled)
          )
        },
        test("rejects unknown statuses") {
          assert(UserStatus.from("banned"))(isLeft(isSubtype[InvalidValue](anything)))
        }
      ),
      suite("Password.from")(
        test("accepts a non-empty password") {
          assertTrue(Password.from("secret123").isRight)
        },
        test("rejects an empty password") {
          assert(Password.from(""))(isLeft(equalTo(InvalidValue("password", "Password must not be empty"))))
        }
      ),
      suite("PasswordHash.from")(
        test("accepts a 60-character bcrypt string") {
          val hash = "$2a$12$abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0"
          assertTrue(PasswordHash.from(hash) == Right(PasswordHash(hash)))
        },
        test("rejects a non-bcrypt string") {
          assert(PasswordHash.from("plain"))(isLeft(isSubtype[InvalidValue](anything)))
        }
      ),
      suite("UserId.from")(
        test("parses a valid UUID") {
          val raw = "5d4e68f1-2f1a-4f3c-9c2b-1234567890ab"
          assertTrue(UserId.from(raw) == Right(UserId(UUID.fromString(raw))))
        },
        test("rejects a malformed UUID") {
          assert(UserId.from("nope"))(isLeft(isSubtype[InvalidValue](anything)))
        }
      ),
      suite("User.create")(
        test("creates an active user and trims the name") {
          val now = Instant.parse("2026-01-01T00:00:00Z")
          val email = Email("popug@ates.io")
          val hash = PasswordHash("$2a$12$abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0")
          val user = User.create(UserId(UUID.randomUUID()), "  Popug  ", email, hash, Role.Popug, now).toOption.get
          assertTrue(
            user.name == "Popug",
            user.status == UserStatus.Active,
            user.createdAt == now,
            user.updatedAt == now
          )
        },
        test("rejects an empty name") {
          val email = Email("popug@ates.io")
          val hash = PasswordHash("$2a$12$abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0")
          assert(User.create(UserId(UUID.randomUUID()), "   ", email, hash, Role.Popug, Instant.EPOCH))(
            isLeft(equalTo(InvalidValue("name", "Name must not be empty")))
          )
        },
        test("rejects a name longer than 255 characters") {
          val email = Email("popug@ates.io")
          val hash = PasswordHash("$2a$12$abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0")
          assert(User.create(UserId(UUID.randomUUID()), "x" * 256, email, hash, Role.Popug, Instant.EPOCH))(
            isLeft(isSubtype[InvalidValue](anything))
          )
        },
        test("withRole and withStatus update the updatedAt timestamp") {
          val now = Instant.parse("2026-01-01T00:00:00Z")
          val later = now.plusSeconds(60)
          val email = Email("popug@ates.io")
          val hash = PasswordHash("$2a$12$abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0")
          val user = User.create(UserId(UUID.randomUUID()), "Popug", email, hash, Role.Popug, now).toOption.get
          val changed =
            User.withStatus(User.withRole(user, Role.Manager, later), UserStatus.Disabled, later.plusSeconds(1))
          assertTrue(
            changed.role == Role.Manager,
            changed.status == UserStatus.Disabled,
            changed.updatedAt == later.plusSeconds(1)
          )
        }
      ),
      suite("RefreshToken")(
        test("accepts a valid UUID token") {
          val raw = UUID.randomUUID().toString
          assertTrue(RefreshToken.from(raw).map(_.value).toOption.contains(raw))
        },
        test("rejects a malformed token") {
          assert(RefreshToken.from("not-a-uuid"))(isLeft(isSubtype[InvalidValue](anything)))
        },
        test("RefreshTokenHash is deterministic and URL-safe") {
          val token = RefreshToken.from("7c1f9d0e-6b2a-4e8f-9d4c-abcdef123456").toOption.get
          assertTrue(
            RefreshTokenHash.of(token) == RefreshTokenHash.of(token),
            !RefreshTokenHash.of(token).value.contains("+")
          )
        }
      ),
      suite("DomainError taxonomy")(
        test("token errors carry messages") {
          assertTrue(
            TokenInvalid("bad").isInstanceOf[TokenInvalid],
            RefreshTokenInvalid("rotated").isInstanceOf[RefreshTokenInvalid]
          )
        }
      )
    )
