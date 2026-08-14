package inc.uberpopug.auth.domain

import java.security.MessageDigest
import java.util.{Base64, UUID}

import scala.util.Try

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Refresh-токен: UUIDv4, хранится клиентом в неизменном виде. */
opaque type RefreshToken = String

object RefreshToken:
  /** Генерирует новый refresh-токен (UUIDv4). */
  def generate(): RefreshToken = UUID.randomUUID().toString

  /** Smart-конструктор: принимает токен, только если он — корректный UUID. */
  def from(value: String): Either[DomainError, RefreshToken] =
    Try(UUID.fromString(value)).toEither.left
      .map(_ => InvalidValue("refreshToken", s"Invalid refresh token: '$value'"))
      .map(_ => value)

  /** Доступ к сырому строковому значению. */
  extension (token: RefreshToken) def value: String = token

/** SHA-256-хэш refresh-токена — то, что реально хранится в БД. */
opaque type RefreshTokenHash = String

object RefreshTokenHash:
  /** Считает URL-safe base64 от SHA-256 хэша токена (без padding). */
  def of(token: RefreshToken): RefreshTokenHash =
    val digest = MessageDigest.getInstance("SHA-256").digest(token.value.getBytes("UTF-8"))
    RefreshTokenHash(Base64.getUrlEncoder.withoutPadding().encodeToString(digest))

  /** Непроверяющий конструктор. */
  def apply(value: String): RefreshTokenHash = value

  /** Доступ к сырому строковому значению. */
  extension (hash: RefreshTokenHash) def value: String = hash
