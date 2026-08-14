package inc.uberpopug.auth.api

import sttp.model.StatusCode
import sttp.tapir.ztapir.*
import zio.{Clock, ZIO, ZLayer}

import inc.uberpopug.auth.domain.{Role, User, UserStatus}
import inc.uberpopug.auth.service.{AuthService, AuthenticatedUser, Jwk, TokenPair, TokenService}
import inc.uberpopug.common.domain.UserId

/** Server logic: связывает tapir-эндпоинты с сервисами и маппингом ошибок. */
final case class AuthServerLogic(authService: AuthService, tokenService: TokenService):
  import AuthEndpoints.*
  import AuthServerLogic.*

  /** Верифицирует Bearer-токен и возвращает аутентифицированного пользователя. */
  private def security(token: String): ZIO[Clock, (StatusCode, ErrorResponse), AuthenticatedUser] =
    tokenService.verifyAccess(token).mapError(ErrorMapping.toApiError)

  /** Публичные эндпоинты: login, refresh, logout, health, ready, keys. */
  private val publicEndpoints: List[ZServerEndpoint[Clock, Any]] =
    List(
      login.zServerLogic[Clock] { req =>
        authService.login(req.login, req.password).mapError(ErrorMapping.toApiError).map(toTokenResponse)
      },
      refresh.zServerLogic[Clock] { req =>
        authService.refresh(req.refreshToken).mapError(ErrorMapping.toApiError).map(toTokenResponse)
      },
      logout.zServerLogic[Clock] { req =>
        authService.logout(req.refreshToken).mapError(ErrorMapping.toApiError)
      },
      health.zServerLogic[Clock](_ => ZIO.succeed(HealthResponse("ok"))),
      ready.zServerLogic[Clock](_ => ZIO.succeed(HealthResponse("ok"))),
      keys.zServerLogic[Clock](_ => ZIO.succeed(JwksResponse(List(toJwkDto(tokenService.publicJwk)))))
    )

  /** Админ-эндпоинты: создание, список, получение и обновление пользователей. */
  private val adminEndpoints: List[ZServerEndpoint[Clock, Any]] =
    List(
      createUser
        .zServerSecurityLogic(security)
        .serverLogic[Clock] { actor => req =>
          for
            role <- ZIO.fromEither(Role.from(req.role)).mapError(ErrorMapping.toApiError)
            user <- authService
              .createUser(req.name, req.email, req.password, role, actor)
              .mapError(ErrorMapping.toApiError)
          yield toUserResponse(user)
        },
      listUsers
        .zServerSecurityLogic(security)
        .serverLogic[Clock] { actor => (limit, offset) =>
          for users <- authService
              .listUsers(limit.getOrElse(DefaultLimit), offset.getOrElse(0), actor)
              .mapError(ErrorMapping.toApiError)
          yield UsersResponse(users.map(toUserResponse))
        },
      getUser
        .zServerSecurityLogic(security)
        .serverLogic[Clock] { actor => id =>
          for
            userId <- ZIO.fromEither(UserId.from(id.toString)).mapError(ErrorMapping.toApiError)
            user <- authService.getUser(userId, actor).mapError(ErrorMapping.toApiError)
          yield toUserResponse(user)
        },
      updateUser
        .zServerSecurityLogic(security)
        .serverLogic[Clock] { actor => (id, req) =>
          for
            userId <- ZIO.fromEither(UserId.from(id.toString)).mapError(ErrorMapping.toApiError)
            role <- ZIO.foreach(req.role)(r => ZIO.fromEither(Role.from(r))).mapError(ErrorMapping.toApiError)
            status <- ZIO.foreach(req.status)(s => ZIO.fromEither(UserStatus.from(s))).mapError(ErrorMapping.toApiError)
            user <- authService.updateUser(userId, role, status, actor).mapError(ErrorMapping.toApiError)
          yield toUserResponse(user)
        }
    )

  /** Все эндпоинты сервиса для HTTP-сервера. */
  val endpoints: List[ZServerEndpoint[Clock, Any]] = publicEndpoints ++ adminEndpoints

object AuthServerLogic:
  /** Дефолтный размер страницы списка пользователей. */
  val DefaultLimit = 50

  /** Маппит доменного пользователя в DTO ответа. */
  def toUserResponse(user: User): UserResponse =
    UserResponse(
      id = user.id.value.toString,
      name = user.name,
      email = user.email.value,
      role = user.role.wire,
      status = user.status.wire,
      createdAt = user.createdAt,
      updatedAt = user.updatedAt
    )

  /** Маппит пару токенов в DTO ответа (Bearer, `expires_in`). */
  def toTokenResponse(pair: TokenPair): TokenResponse =
    TokenResponse(
      accessToken = pair.accessToken.value,
      refreshToken = pair.refreshToken.value,
      tokenType = "Bearer",
      expiresIn = pair.accessToken.expiresInSeconds
    )

  /** Маппит доменный JWK в DTO ответа. */
  def toJwkDto(jwk: Jwk): JwkDto =
    JwkDto(jwk.kid, jwk.kty, jwk.crv, jwk.x, jwk.y, jwk.alg, jwk.use)

  /** Слой server logic поверх AuthService и TokenService. */
  val layer: ZLayer[AuthService & TokenService, Nothing, AuthServerLogic] =
    ZLayer.fromFunction(AuthServerLogic(_, _))
