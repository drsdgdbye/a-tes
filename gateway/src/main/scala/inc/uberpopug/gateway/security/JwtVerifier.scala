package inc.uberpopug.gateway.security

import java.security.interfaces.ECPublicKey
import scala.util.Try

import com.nimbusds.jose.crypto.ECDSAVerifier
import com.nimbusds.jose.jwk.Curve
import com.nimbusds.jose.jwk.ECKey
import com.nimbusds.jose.util.Base64URL
import com.nimbusds.jwt.SignedJWT
import zio.{Clock, ZIO}
import zio.json.{DeriveJsonCodec, JsonCodec}

import inc.uberpopug.common.domain.UserId
import inc.uberpopug.gateway.security.JwtVerificationError.*

/** Результат верификации access-токена: аутентифицированный пользователь и роль из claims. */
final case class Authenticated(id: UserId, role: Role)

/** Публичный JWK из ответа `GET /auth/keys`. */
final case class JwkDto(kid: String, kty: String, crv: String, x: String, y: String, alg: String, use: String)

/** Ответ `GET /auth/keys` — список публичных JWK. */
final case class JwksResponse(keys: List[JwkDto])

object JwksResponse:
  given JsonCodec[JwkDto] = DeriveJsonCodec.gen
  given JsonCodec[JwksResponse] = DeriveJsonCodec.gen

/** Локальная верификация подписи ES256 и claims access-токена по публичному ключу Auth, без network call. */
final case class JwtVerifier(keyId: String, verifier: ECDSAVerifier, issuer: String):
  /** Верифицирует токен: подпись, issuer, срок действия, subject и роль в claims; нарушение — соответствующий
    * `JwtVerificationError`.
    */
  def verify(token: String): ZIO[Clock, JwtVerificationError, Authenticated] =
    for
      now <- Clock.instant
      jwt <- ZIO.attempt(SignedJWT.parse(token)).mapError(ex => InvalidToken(ex.getMessage))
      valid <- ZIO.attempt(jwt.verify(verifier)).mapError(ex => InvalidToken(ex.getMessage))
      _ <- if !valid then ZIO.fail(InvalidToken("Invalid token signature")) else ZIO.unit
      claims = jwt.getJWTClaimsSet
      issuerOk <- ZIO.attempt(claims.getIssuer == issuer).mapError(ex => InvalidToken(ex.getMessage))
      _ <- if !issuerOk then ZIO.fail(InvalidIssuer) else ZIO.unit
      expiresAt <- ZIO.attempt(claims.getExpirationTime).mapError(ex => InvalidToken(ex.getMessage))
      _ <-
        if expiresAt == null || now.isAfter(expiresAt.toInstant) then ZIO.fail(TokenExpired)
        else ZIO.unit
      sub <- ZIO.fromEither(UserId.from(claims.getSubject)).mapError(_ => InvalidSubject)
      role <- ZIO
        .attempt(claims.getStringClaim("role"))
        .mapError(ex => InvalidToken(ex.getMessage))
        .flatMap(value => ZIO.fromEither(Role.from(value)).mapError(_ => InvalidRole))
    yield Authenticated(sub, role)

object JwtVerifier:
  /** Строит верификатор из первого JWK ответа `GET /auth/keys` (EC P-256, x/y → публичный ключ → ECDSAVerifier). */
  def fromJwks(jwks: JwksResponse, issuer: String): Either[String, JwtVerifier] =
    jwks.keys.headOption.toRight("Auth keys response is empty").flatMap(jwk => fromJwk(jwk, issuer))

  /** Строит верификатор из одного JWK. */
  def fromJwk(jwk: JwkDto, issuer: String): Either[String, JwtVerifier] =
    for
      curve <- Try(Curve.parse(jwk.crv)).toEither.left.map(_.getMessage)
      ecKey <- Try(new ECKey.Builder(curve, new Base64URL(jwk.x), new Base64URL(jwk.y)).build()).toEither.left
        .map(_.getMessage)
      publicKey <- Try(ecKey.toPublicKey.asInstanceOf[ECPublicKey]).toEither.left.map(_.getMessage)
      verifier <- Try(new ECDSAVerifier(publicKey)).toEither.left.map(_.getMessage)
    yield JwtVerifier(jwk.kid, verifier, issuer)
