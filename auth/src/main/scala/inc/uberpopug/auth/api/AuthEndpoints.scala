package inc.uberpopug.auth.api

import java.util.UUID

import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*

/** Декларативное описание всех HTTP-эндпоинтов сервиса через tapir. */
object AuthEndpoints:
  import AuthDtos.given

  /** Единый формат ошибки: HTTP-статус + JSON-тело `ErrorResponse`. */
  private val jsonErrorOut: EndpointOutput[(StatusCode, ErrorResponse)] =
    statusCode.and(jsonBody[ErrorResponse])

  /** `GET /health` — liveness-проверка. */
  val health: PublicEndpoint[Unit, Unit, HealthResponse, Any] =
    endpoint.get.in("health").out(jsonBody[HealthResponse])

  /** `GET /ready` — readiness-проверка. */
  val ready: PublicEndpoint[Unit, Unit, HealthResponse, Any] =
    endpoint.get.in("ready").out(jsonBody[HealthResponse])

  /** `POST /auth/login` — аутентификация и выдача токенов. */
  val login: PublicEndpoint[LoginRequest, (StatusCode, ErrorResponse), TokenResponse, Any] =
    endpoint.post
      .in("auth" / "login")
      .in(jsonBody[LoginRequest])
      .out(jsonBody[TokenResponse])
      .errorOut(jsonErrorOut)

  /** `POST /auth/refresh` — ротация refresh-токена. */
  val refresh: PublicEndpoint[RefreshRequest, (StatusCode, ErrorResponse), TokenResponse, Any] =
    endpoint.post
      .in("auth" / "refresh")
      .in(jsonBody[RefreshRequest])
      .out(jsonBody[TokenResponse])
      .errorOut(jsonErrorOut)

  /** `POST /auth/logout` — отзыв refresh-токена (204). */
  val logout: PublicEndpoint[LogoutRequest, (StatusCode, ErrorResponse), Unit, Any] =
    endpoint.post
      .in("auth" / "logout")
      .in(jsonBody[LogoutRequest])
      .out(statusCode(StatusCode.NoContent))
      .errorOut(jsonErrorOut)

  /** `GET /auth/keys` — публичные JWK для верификации JWT. */
  val keys: PublicEndpoint[Unit, Unit, JwksResponse, Any] =
    endpoint.get.in("auth" / "keys").out(jsonBody[JwksResponse])

  /** Общая security-схема защищённых эндпоинтов: Bearer-токен. */
  private val secured = endpoint.securityIn(auth.bearer[String]())

  /** `POST /users` — создание пользователя (admin). */
  val createUser: Endpoint[String, CreateUserRequest, (StatusCode, ErrorResponse), UserResponse, Any] =
    secured.post
      .in("users")
      .in(jsonBody[CreateUserRequest])
      .out(jsonBody[UserResponse])
      .errorOut(jsonErrorOut)

  /** `GET /users` — список пользователей с пагинацией (admin). */
  val listUsers: Endpoint[String, (Option[Int], Option[Int]), (StatusCode, ErrorResponse), UsersResponse, Any] =
    secured.get
      .in("users")
      .in(query[Option[Int]]("limit"))
      .in(query[Option[Int]]("offset"))
      .out(jsonBody[UsersResponse])
      .errorOut(jsonErrorOut)

  /** `GET /users/{id}` — пользователь по id (любой аутентифицированный). */
  val getUser: Endpoint[String, UUID, (StatusCode, ErrorResponse), UserResponse, Any] =
    secured.get
      .in("users" / path[UUID])
      .out(jsonBody[UserResponse])
      .errorOut(jsonErrorOut)

  /** `PATCH /users/{id}` — изменение роли/статуса (admin). */
  val updateUser: Endpoint[String, (UUID, UpdateUserRequest), (StatusCode, ErrorResponse), UserResponse, Any] =
    secured.patch
      .in("users" / path[UUID])
      .in(jsonBody[UpdateUserRequest])
      .out(jsonBody[UserResponse])
      .errorOut(jsonErrorOut)

  /** Все эндпоинты сервиса для монтажа в HTTP-сервер. */
  val all: List[AnyEndpoint] =
    List(health, ready, login, refresh, logout, keys, createUser, listUsers, getUser, updateUser)
