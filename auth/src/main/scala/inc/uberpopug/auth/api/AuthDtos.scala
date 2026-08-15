package inc.uberpopug.auth.api

import java.time.Instant

import zio.json.*

/** Тело запроса `POST /auth/login`. */
final case class LoginRequest(login: String, password: String)

/** Тело запроса `POST /auth/register` (саморегистрация). */
final case class RegisterRequest(name: String, email: String, password: String)

/** Ответ `GET /auth/config` — публичные capabilities сервиса аутентификации. */
final case class AuthConfigResponse(registrationEnabled: Boolean)

/** Тело запроса `POST /auth/refresh`. */
final case class RefreshRequest(refreshToken: String)

/** Тело запроса `POST /auth/logout`. */
final case class LogoutRequest(refreshToken: String)

/** Тело запроса `POST /users` (создание пользователя админом). */
final case class CreateUserRequest(name: String, email: String, password: String, role: String)

/** Тело запроса `POST /users/me/password` (смена своего пароля). */
final case class ChangePasswordRequest(currentPassword: String, newPassword: String)

/** Тело запроса `PATCH /users/{id}/password` (сброс пароля админом). */
final case class ResetPasswordRequest(newPassword: String)

/** Тело запроса `PATCH /users/{id}`: опциональные роль и статус. */
final case class UpdateUserRequest(role: Option[String], status: Option[String])

/** Ответ `GET/POST/PATCH /users...` — представление пользователя в API. */
final case class UserResponse(
    id: String,
    name: String,
    email: String,
    role: String,
    status: String,
    createdAt: Instant,
    updatedAt: Instant
)

/** Ответ `GET /users` — страница списка пользователей. */
final case class UsersResponse(items: List[UserResponse])

/** Ответ логина/refresh — пара токенов и срок жизни access-токена. */
final case class TokenResponse(accessToken: String, refreshToken: String, tokenType: String, expiresIn: Long)

/** Параметры публичного ключа в `GET /auth/keys`. */
final case class JwkDto(kid: String, kty: String, crv: String, x: String, y: String, alg: String, use: String)

/** Ответ `GET /auth/keys` — список публичных JWK. */
final case class JwksResponse(keys: List[JwkDto])

/** Ответ liveness/readiness эндпоинтов. */
final case class HealthResponse(status: String)

object AuthDtos:
  given JsonCodec[ErrorResponse] = DeriveJsonCodec.gen
  given JsonCodec[LoginRequest] = DeriveJsonCodec.gen
  given JsonCodec[RegisterRequest] = DeriveJsonCodec.gen
  given JsonCodec[AuthConfigResponse] = DeriveJsonCodec.gen
  given JsonCodec[RefreshRequest] = DeriveJsonCodec.gen
  given JsonCodec[LogoutRequest] = DeriveJsonCodec.gen
  given JsonCodec[CreateUserRequest] = DeriveJsonCodec.gen
  given JsonCodec[ChangePasswordRequest] = DeriveJsonCodec.gen
  given JsonCodec[ResetPasswordRequest] = DeriveJsonCodec.gen
  given JsonCodec[UpdateUserRequest] = DeriveJsonCodec.gen
  given JsonCodec[UserResponse] = DeriveJsonCodec.gen
  given JsonCodec[UsersResponse] = DeriveJsonCodec.gen
  given JsonCodec[TokenResponse] = DeriveJsonCodec.gen
  given JsonCodec[JwkDto] = DeriveJsonCodec.gen
  given JsonCodec[JwksResponse] = DeriveJsonCodec.gen
  given JsonCodec[HealthResponse] = DeriveJsonCodec.gen
