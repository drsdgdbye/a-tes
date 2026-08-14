package inc.uberpopug.auth.service

import java.security.KeyPairGenerator
import java.security.interfaces.{ECPrivateKey, ECPublicKey}
import java.security.spec.ECGenParameterSpec
import java.time.Instant
import java.util.{Date, UUID}

import com.nimbusds.jose.crypto.{ECDSAVerifier, ECDSASigner}
import com.nimbusds.jose.jwk.{Curve, KeyUse}
import com.nimbusds.jose.{JOSEObjectType, JWSAlgorithm, JWSHeader}
import com.nimbusds.jwt.{JWTClaimsSet, SignedJWT}
import zio.{Clock, ZIO, ZLayer}

import inc.uberpopug.auth.config.JwtConfig
import inc.uberpopug.auth.domain.{RefreshToken, Role}
import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.UserId
import inc.uberpopug.common.domain.DomainError.TokenInvalid

/** Подписанный access-токен и его срок жизни в секундах (для `expires_in`). */
final case class AccessToken(value: String, expiresInSeconds: Long)

/** Результат выпуска refresh-токена: сам токен и момент истечения. */
final case class IssuedRefreshToken(token: RefreshToken, expiresAt: Instant)

/** Аутентифицированный пользователь после верификации access-токена. */
final case class AuthenticatedUser(id: UserId, role: Role)

/** Публичные параметры ключа подписи JWT (для эндпоинта `GET /auth/keys`). */
final case class Jwk(kid: String, kty: String, crv: String, x: String, y: String, alg: String, use: String)

/** Выпуск и верификация JWT (ES256). */
trait TokenService:
  /** Выпускает access-токен для пользователя с ролью (срок из конфига). */
  def issueAccess(id: UserId, role: Role): ZIO[Clock, DomainError, AccessToken]

  /** Выпускает refresh-токен (UUIDv4) со сроком жизни из конфига. */
  def issueRefresh(id: UserId): ZIO[Clock, DomainError, IssuedRefreshToken]

  /** Верифицирует access-токен: подпись, issuer, срок, роль, subject. */
  def verifyAccess(token: String): ZIO[Clock, DomainError, AuthenticatedUser]

  /** Публичный JWK для локальной верификации подписи на стороне Gateway. */
  def publicJwk: Jwk

object TokenService:
  /** Слой сервиса: при старте генерирует эфемерную пару EC P-256 (secp256r1) и создаёт реализацию с
    * подписью/верификацией на этом ключе.
    */
  val layer: ZLayer[JwtConfig, Throwable, TokenService] =
    ZLayer.fromZIO {
      for
        cfg <- ZIO.service[JwtConfig]
        keyPair <- ZIO.attempt(generateKeyPair())
        _ <- ZIO.logInfo("Generated ephemeral EC P-256 key pair for JWT signing")
      yield TokenServiceLive(cfg, keyPair)
    }

  /** Генерирует пару ключей EC на кривой secp256r1 (P-256). */
  private def generateKeyPair(): java.security.KeyPair =
    val generator = KeyPairGenerator.getInstance("EC")
    generator.initialize(new ECGenParameterSpec("secp256r1"))
    generator.generateKeyPair()

/** Реализация TokenService на nimbus-jose-jwt с заданными конфигом и ключом. */
final case class TokenServiceLive(cfg: JwtConfig, keyPair: java.security.KeyPair) extends TokenService:
  private val signer = new ECDSASigner(keyPair.getPrivate.asInstanceOf[ECPrivateKey])
  private val verifier = new ECDSAVerifier(keyPair.getPublic.asInstanceOf[ECPublicKey])
  private val keyId = s"ates-auth-${UUID.randomUUID().toString.take(8)}"

  /** Строит публичный JWK из открытого ключа: kid, кривая, алгоритм ES256, use=sig. */
  val publicJwk: Jwk =
    val ecKey = new com.nimbusds.jose.jwk.ECKey.Builder(Curve.P_256, keyPair.getPublic.asInstanceOf[ECPublicKey])
      .keyID(keyId)
      .algorithm(JWSAlgorithm.ES256)
      .keyUse(KeyUse.SIGNATURE)
      .build()
    val pub = ecKey.toPublicJWK()
    Jwk(
      kid = pub.getKeyID,
      kty = pub.getKeyType.getValue,
      crv = pub.getCurve.getName,
      x = pub.getX.toString,
      y = pub.getY.toString,
      alg = pub.getAlgorithm.getName,
      use = pub.getKeyUse.getValue
    )

  /** Собирает claims (sub, role, iss, iat, exp) и подписывает JWT алгоритмом ES256. */
  def issueAccess(id: UserId, role: Role): ZIO[Clock, DomainError, AccessToken] =
    for
      now <- Clock.instant
      claims = new JWTClaimsSet.Builder()
        .subject(id.value.toString)
        .claim("role", role.wire)
        .issuer(cfg.issuer)
        .issueTime(Date.from(now))
        .expirationTime(Date.from(now.plusSeconds(cfg.accessTtlSeconds)))
        .build()
      header = new JWSHeader.Builder(JWSAlgorithm.ES256).`type`(JOSEObjectType.JWT).build()
      jwt = new SignedJWT(header, claims)
      _ <- ZIO.attemptBlocking(jwt.sign(signer)).mapError(ex => TokenInvalid(ex.getMessage))
    yield AccessToken(jwt.serialize(), cfg.accessTtlSeconds)

  /** Выпускает refresh-токен: случайный UUIDv4 и срок жизни из конфига. */
  def issueRefresh(id: UserId): ZIO[Clock, DomainError, IssuedRefreshToken] =
    for now <- Clock.instant
    yield IssuedRefreshToken(RefreshToken.generate(), now.plusSeconds(cfg.refreshTtlSeconds))

  /** Верифицирует access-токен: парсит JWT, проверяет подпись, issuer, срок действия, роль в claims и subject; любое
    * нарушение — `TokenInvalid`.
    */
  def verifyAccess(token: String): ZIO[Clock, DomainError, AuthenticatedUser] =
    for
      now <- Clock.instant
      jwt <- ZIO.attempt(SignedJWT.parse(token)).mapError(ex => TokenInvalid(ex.getMessage))
      valid <- ZIO.attempt(jwt.verify(verifier)).mapError(ex => TokenInvalid(ex.getMessage))
      _ <- if !valid then ZIO.fail(TokenInvalid("Invalid token signature")) else ZIO.unit
      claims = jwt.getJWTClaimsSet
      role <- ZIO
        .attempt(claims.getStringClaim("role"))
        .mapError(ex => TokenInvalid(ex.getMessage))
        .flatMap(value => ZIO.fromEither(Role.from(value)).mapError(_ => TokenInvalid("Invalid role")))
      issuerOk <- ZIO.attempt(claims.getIssuer == cfg.issuer).mapError(ex => TokenInvalid(ex.getMessage))
      _ <- if !issuerOk then ZIO.fail(TokenInvalid("Invalid token issuer")) else ZIO.unit
      expiresAt <- ZIO.attempt(claims.getExpirationTime).mapError(ex => TokenInvalid(ex.getMessage))
      _ <-
        if expiresAt == null || now.isAfter(expiresAt.toInstant) then ZIO.fail(TokenInvalid("Token expired"))
        else ZIO.unit
      userId <- ZIO.fromEither(UserId.from(claims.getSubject)).mapError(_ => TokenInvalid("Invalid subject"))
    yield AuthenticatedUser(userId, role)
