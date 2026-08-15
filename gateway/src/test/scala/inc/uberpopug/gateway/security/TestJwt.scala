package inc.uberpopug.gateway.security

import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.interfaces.{ECPrivateKey, ECPublicKey}
import java.security.spec.ECGenParameterSpec
import java.util.Date

import com.nimbusds.jose.crypto.ECDSASigner
import com.nimbusds.jose.jwk.{Curve, ECKey}
import com.nimbusds.jose.{JWSAlgorithm, JWSHeader}
import com.nimbusds.jwt.{JWTClaimsSet, SignedJWT}

/** Тестовый набор ES256: публичный JWK, верификатор Gateway и подпись access-токенов для проверки JWT-гейта. */
final case class TestJwt(keyPair: KeyPair, verifier: JwtVerifier, jwk: JwkDto):
  /** JWKS-ответ Auth для этого ключа. */
  def jwks: JwksResponse = JwksResponse(List(jwk))

  /** Подписывает access-токен с произвольными claims. */
  def sign(subject: String, issuer: String, role: String, expiresAt: Date): String =
    val signer = new ECDSASigner(keyPair.getPrivate.asInstanceOf[ECPrivateKey])
    val header = new JWSHeader.Builder(JWSAlgorithm.ES256).keyID(jwk.kid).build()
    val claims = new JWTClaimsSet.Builder()
      .subject(subject)
      .issuer(issuer)
      .expirationTime(expiresAt)
      .claim("role", role)
      .build()
    val jwt = new SignedJWT(header, claims)
    jwt.sign(signer)
    jwt.serialize()

object TestJwt:
  /** Создаёт тестовые ключи и верификатор для заданного issuer. */
  def make(issuer: String, kid: String = "test-kid"): TestJwt =
    val keyPair = generateKeyPair
    val publicKey = keyPair.getPublic.asInstanceOf[ECPublicKey]
    val ecKey = new ECKey.Builder(Curve.P_256, publicKey).keyID(kid).build()
    val dto = JwkDto(ecKey.getKeyID, "EC", "P-256", ecKey.getX.toString, ecKey.getY.toString, "ES256", "sig")
    TestJwt(keyPair, JwtVerifier.fromJwk(dto, issuer).toOption.get, dto)

  private def generateKeyPair: KeyPair =
    val generator = KeyPairGenerator.getInstance("EC")
    generator.initialize(new ECGenParameterSpec("secp256r1"))
    generator.generateKeyPair()
