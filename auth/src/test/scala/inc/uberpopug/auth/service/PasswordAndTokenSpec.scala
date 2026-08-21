package inc.uberpopug.auth.service

import java.security.KeyPairGenerator
import java.security.interfaces.ECPrivateKey
import java.security.spec.ECGenParameterSpec
import java.time.Instant
import java.util.{Date, UUID}

import com.nimbusds.jose.crypto.ECDSASigner
import com.nimbusds.jose.{JOSEObjectType, JWSAlgorithm, JWSHeader}
import com.nimbusds.jwt.{JWTClaimsSet, SignedJWT}
import zio.*
import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.auth.config.JwtConfig
import inc.uberpopug.auth.domain.Password
import inc.uberpopug.auth.domain.PasswordHash
import inc.uberpopug.auth.domain.Role
import inc.uberpopug.common.domain.DomainError.TokenInvalid
import inc.uberpopug.common.domain.UserId

/** Тесты PasswordHasher: bcrypt hash/verify. */
object PasswordHasherSpec extends ZIOSpecDefault:
  def spec: Spec[Any, Any] =
    suite("PasswordHasher")(
      test("hash then verify returns true for the original password") {
        for
          password <- ZIO.succeed(Password.from("correct horse battery staple").toOption.get)
          hash <- PasswordHasherLive.hash(password)
          ok <- PasswordHasherLive.verify(password, hash)
        yield assertTrue(ok, hash.value.length == PasswordHash.BcryptLength)
      },
      test("verify returns false for a wrong password") {
        for
          password <- ZIO.succeed(Password.from("secret123").toOption.get)
          hash <- PasswordHasherLive.hash(password)
          ok <- PasswordHasherLive.verify(Password.from("wrong").toOption.get, hash)
        yield assertTrue(!ok)
      }
    )

/** Тесты TokenService: roundtrip, подпись, TTL, JWK, ротация refresh. */
object TokenServiceSpec extends ZIOSpecDefault:
  /** Тестовый JWT-конфиг для детерминированных TTL-проверок. */
  private val cfg = JwtConfig(issuer = "ates-test", accessTtlSeconds = 900, refreshTtlSeconds = 604800)

  /** Генерирует тестовую пару ключей EC P-256. */
  private def newKeyPair: java.security.KeyPair =
    val generator = KeyPairGenerator.getInstance("EC")
    generator.initialize(new ECGenParameterSpec("secp256r1"))
    generator.generateKeyPair()

  /** Снижает env-требование `Clock` до `Any` (время управляется TestClock). */
  private def withLiveClock[E, A](effect: ZIO[Clock, E, A]): ZIO[Any, E, A] =
    effect.provideLayer(ZLayer.succeed(Clock.ClockLive))

  def spec: Spec[Any, Any] =
    suite("TokenService")(
      test("issueAccess then verifyAccess roundtrip returns the subject and role") {
        val id = UserId(UUID.randomUUID())
        val service = TokenServiceLive(cfg, newKeyPair)
        withLiveClock {
          for
            token <- service.issueAccess(id, Role.Manager)
            auth <- service.verifyAccess(token.value)
          yield assertTrue(auth.id == id, auth.role == Role.Manager)
        }
      },
      test("verifyAccess rejects a token signed by a different key") {
        val id = UserId(UUID.randomUUID())
        val service = TokenServiceLive(cfg, newKeyPair)
        val other = TokenServiceLive(cfg, newKeyPair)
        withLiveClock {
          for
            token <- service.issueAccess(id, Role.Popug)
            result <- other.verifyAccess(token.value).exit
          yield assert(result)(fails(isSubtype[TokenInvalid](anything)))
        }
      },
      test("access token expires after its TTL") {
        val id = UserId(UUID.randomUUID())
        val service = TokenServiceLive(cfg, newKeyPair)
        withLiveClock {
          for
            token <- service.issueAccess(id, Role.Popug)
            _ <- TestClock.adjust(cfg.accessTtlSeconds.seconds + 1.second)
            result <- service.verifyAccess(token.value).exit
          yield assert(result)(fails(isSubtype[TokenInvalid](anything)))
        }
      },
      test("publicJwk exposes an ES256 key with all fields") {
        val jwk = TokenServiceLive(cfg, newKeyPair).publicJwk
        assertTrue(
          jwk.kty == "EC",
          jwk.crv == "P-256",
          jwk.alg == "ES256",
          jwk.use == "sig",
          jwk.kid.nonEmpty,
          jwk.x.nonEmpty,
          jwk.y.nonEmpty
        )
      },
      test("issueRefresh produces a distinct token with a TTL-based expiry") {
        val service = TokenServiceLive(cfg, newKeyPair)
        withLiveClock {
          for
            refresh1 <- service.issueRefresh(UserId(UUID.randomUUID()))
            refresh2 <- service.issueRefresh(UserId(UUID.randomUUID()))
          yield assertTrue(
            refresh1.token.value != refresh2.token.value,
            refresh1.expiresAt == java.time.Instant.EPOCH.plusSeconds(cfg.refreshTtlSeconds)
          )
        }
      },
      suite("access token exp boundary")(
        test("exp exactly now is accepted (M-AUTH-15)") {
          val id = UserId(UUID.randomUUID())
          val kp = newKeyPair
          val service = TokenServiceLive(cfg, kp)
          withLiveClock {
            for
              now <- Clock.instant
              token = signToken(kp, id, Role.Popug, Date.from(now))
              result <- service.verifyAccess(token).exit
            yield assertTrue(result.isSuccess)
          }
        },
        test("exp one second in the future is accepted (M-AUTH-16)") {
          val id = UserId(UUID.randomUUID())
          val kp = newKeyPair
          val service = TokenServiceLive(cfg, kp)
          withLiveClock {
            for
              now <- Clock.instant
              token = signToken(kp, id, Role.Popug, Date.from(now.plusSeconds(1)))
              result <- service.verifyAccess(token).exit
            yield assertTrue(result.isSuccess)
          }
        },
        test("exp one second in the past is rejected as expired (M-AUTH-15)") {
          val id = UserId(UUID.randomUUID())
          val kp = newKeyPair
          val service = TokenServiceLive(cfg, kp)
          withLiveClock {
            for
              now <- Clock.instant
              token = signToken(kp, id, Role.Popug, Date.from(now.minusSeconds(1)))
              result <- service.verifyAccess(token).exit
            yield assert(result)(fails(isSubtype[TokenInvalid](anything)))
          }
        }
      )
    )

  /** Подписывает access-токен ES256 тем же ключом, что использует `TokenServiceLive`, с заданным `exp`. */
  private def signToken(kp: java.security.KeyPair, id: UserId, role: Role, exp: Date): String =
    val signer = new ECDSASigner(kp.getPrivate.asInstanceOf[ECPrivateKey])
    val claims = new JWTClaimsSet.Builder()
      .subject(id.value.toString)
      .claim("role", role.wire)
      .issuer(cfg.issuer)
      .issueTime(Date.from(Instant.EPOCH))
      .expirationTime(exp)
      .build()
    val header = new JWSHeader.Builder(JWSAlgorithm.ES256).`type`(JOSEObjectType.JWT).build()
    val jwt = new SignedJWT(header, claims)
    jwt.sign(signer)
    jwt.serialize()
