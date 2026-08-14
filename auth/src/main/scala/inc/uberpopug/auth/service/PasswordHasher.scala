package inc.uberpopug.auth.service

import at.favre.lib.crypto.bcrypt.BCrypt
import zio.{ZIO, ZLayer}

import inc.uberpopug.auth.domain.{Password, PasswordHash}

/** Хэширование и проверка паролей через bcrypt. */
trait PasswordHasher:
  /** Хэширует пароль bcrypt (cost 12); результат — строка из 60 символов. */
  def hash(password: Password): ZIO[Any, Nothing, PasswordHash]

  /** Проверяет, что пароль соответствует хэшу. */
  def verify(password: Password, hash: PasswordHash): ZIO[Any, Nothing, Boolean]

object PasswordHasher:
  /** Слой с фиксированной реализацией bcrypt. */
  val layer: ZLayer[Any, Nothing, PasswordHasher] = ZLayer.succeed(PasswordHasherLive)

/** Реализация PasswordHasher на at.favre.lib BCrypt. */
object PasswordHasherLive extends PasswordHasher:
  /** Стоимость bcrypt (work factor). */
  private val Cost = 12

  /** Хэширует пароль bcrypt с заданной стоимостью. */
  def hash(password: Password): ZIO[Any, Nothing, PasswordHash] =
    ZIO.succeed(PasswordHash(BCrypt.withDefaults().hashToString(Cost, password.value.toCharArray)))

  /** Проверяет пароль через bcrypt-верификатор. */
  def verify(password: Password, hash: PasswordHash): ZIO[Any, Nothing, Boolean] =
    ZIO.succeed {
      val result = BCrypt.verifyer().verify(password.value.toCharArray, hash.value)
      result.verified
    }
