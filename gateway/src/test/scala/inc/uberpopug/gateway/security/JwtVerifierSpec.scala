package inc.uberpopug.gateway.security

import java.util.Date
import java.util.UUID

import zio.{Clock, ZIO, ZLayer}
import zio.durationInt
import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.gateway.security.JwtVerificationError.*

object JwtVerifierSpec extends ZIOSpecDefault:
  private val issuer = "ates-auth"
  private val jwt = TestJwt.make(issuer)
  private val subject = UUID.randomUUID().toString
  private val future = new Date(System.currentTimeMillis() + 3600_000)

  /** Снижает env `Clock` до `Any`; время чтения — из DefaultServices (TestClock). */
  private def withClock[E, A](effect: ZIO[Clock, E, A]): ZIO[Any, E, A] =
    effect.provideLayer(ZLayer.succeed(Clock.ClockLive))

  def spec: Spec[Any, Any] =
    suite("JwtVerifier")(
      test("verifies a valid token and returns the subject and role") {
        val token = jwt.sign(subject, issuer, "manager", future)
        withClock {
          jwt.verifier.verify(token).map { verified =>
            assertTrue(verified.id.value == UUID.fromString(subject), verified.role == Role.Manager)
          }
        }
      },
      test("rejects a token signed by a different key") {
        val token = TestJwt.make(issuer).sign(subject, issuer, "manager", future)
        withClock(jwt.verifier.verify(token).exit.map(assert(_)(fails(isSubtype[InvalidToken](anything)))))
      },
      test("rejects a token with a wrong issuer") {
        val token = jwt.sign(subject, "evil-issuer", "manager", future)
        withClock(jwt.verifier.verify(token).exit.map(assert(_)(fails(equalTo(InvalidIssuer)))))
      },
      test("rejects a token with an invalid subject") {
        val token = jwt.sign("not-a-uuid", issuer, "manager", future)
        withClock(jwt.verifier.verify(token).exit.map(assert(_)(fails(equalTo(InvalidSubject)))))
      },
      test("rejects a token with an unknown role") {
        val token = jwt.sign(subject, issuer, "supreme-leader", future)
        withClock(jwt.verifier.verify(token).exit.map(assert(_)(fails(equalTo(InvalidRole)))))
      },
      test("rejects a token whose expiration is in the past") {
        val token = jwt.sign(subject, issuer, "manager", new Date(-3600_000L))
        withClock(jwt.verifier.verify(token).exit.map(assert(_)(fails(equalTo(TokenExpired)))))
      },
      test("accepts a not-yet-expired token and rejects it after exp passes") {
        val token = jwt.sign(subject, issuer, "popug", new Date(30_000L))
        for
          before <- withClock(jwt.verifier.verify(token).exit)
          _ <- TestClock.adjust(31.seconds)
          after <- withClock(jwt.verifier.verify(token).exit)
        yield assert(before)(succeeds(anything)) && assert(after)(fails(equalTo(TokenExpired)))
      }
    )
