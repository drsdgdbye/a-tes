package inc.uberpopug.gateway.api

import zio.json.*
import zio.{Chunk, Clock, ZIO, ZEnvironment}
import zio.http.*
import zio.metrics.connectors.prometheus.PrometheusPublisher

import inc.uberpopug.gateway.proxy.*
import inc.uberpopug.gateway.security.{JwtVerificationError, KeyManager}

/** Тело JSON-ошибки Gateway (единый контракт `{ "error": ..., "message": ... }`). */
final case class ErrorBody(error: String, message: String)

object ErrorBody:
  given JsonCodec[ErrorBody] = DeriveJsonCodec.gen

/** Маршруты Gateway: health/ready/metrics + catch-all reverse proxy в downstream-сервисы. */
final case class GatewayRoutes(
    keyManager: KeyManager,
    resilience: Resilience,
    downstreamClient: DownstreamClient,
    prometheus: PrometheusPublisher,
    client: Client,
    clock: Clock
):
  /** Env внутренней программы проксирования: HTTP-клиент для downstream и Clock (JWT-верификация, таймауты). */
  private type GatewayEnv = Client & Clock

  def routes: Routes[Any, Nothing] =
    val health = Method.GET / "health" -> handler(Response.json("""{"status":"ok"}"""))
    val ready =
      Method.GET / "ready" -> handler {
        keyManager.isReady.map {
          case true  => Response.json("""{"status":"ready"}""")
          case false => errorResponse("service_unavailable", "JWT keys not loaded", Status.ServiceUnavailable)
        }
      }
    val metrics = Method.GET / "metrics" -> handler(prometheus.get.map(text => Response.text(text)))

    val routes = Routes(health, ready, metrics)
    routes.notFound = Handler.fromFunctionZIO[Request](proxy)
    routes

  private def proxy(request: Request): ZIO[Any, Nothing, Response] =
    val path = request.url.path.toString
    val program: ZIO[GatewayEnv, Response, Response] =
      for
        body <- request.body.asChunk.mapError(_ =>
          errorResponse("internal_error", "Failed to read request body", Status.InternalServerError)
        )
        downstream <- ZIO
          .fromOption(RouteResolver.resolve(path))
          .mapError(_ => errorResponse("not_found", s"Unknown route: $path", Status.NotFound))
        _ <- checkAuthorization(request, path)
        downstreamRequest <- ZIO
          .fromEither(downstreamClient.build(downstream, request, body))
          .mapError(message => errorResponse("internal_error", message, Status.InternalServerError))
        response <- resilience
          .forDownstream(downstream)
          .protect(request.method, downstreamClient.call(downstreamRequest))
          .catchAll(error => ZIO.succeed(proxyErrorResponse(error)))
      yield response
    program.provideEnvironment(ZEnvironment(client, clock)).catchAll(response => ZIO.succeed(response))

  private def checkAuthorization(request: Request, path: String): ZIO[GatewayEnv, Response, Unit] =
    if RouteResolver.isPublic(path) then ZIO.unit
    else
      keyManager.verifier.flatMap {
        case None => ZIO.fail(errorResponse("service_unavailable", "JWT keys not loaded", Status.ServiceUnavailable))
        case Some(verifier) =>
          bearerToken(request) match
            case None      => ZIO.fail(errorResponse("unauthorized", "Missing bearer token", Status.Unauthorized))
            case Some(raw) => verifier.verify(raw).map(_ => ()).mapError(jwtErrorResponse)
      }

  private def bearerToken(request: Request): Option[String] =
    request.headers.get(Header.Authorization) match
      case Some(Header.Authorization.Bearer(token)) => Some(token.value.asString)
      case _                                        => None

  private def jwtErrorResponse(error: JwtVerificationError): Response =
    val message = error match
      case JwtVerificationError.InvalidToken(details) => s"Invalid token: $details"
      case JwtVerificationError.TokenExpired          => "Token expired"
      case JwtVerificationError.InvalidIssuer         => "Invalid token issuer"
      case JwtVerificationError.InvalidSubject        => "Invalid token subject"
      case JwtVerificationError.InvalidRole           => "Invalid token role"
    errorResponse("unauthorized", message, Status.Unauthorized)

  private def proxyErrorResponse(error: GatewayError): Response = error match
    case GatewayError.DownstreamUnreachable(message) =>
      errorResponse("downstream_unreachable", message, Status.ServiceUnavailable)
    case GatewayError.CallTimedOut =>
      errorResponse("call_timed_out", "Downstream call timed out", Status.GatewayTimeout)
    case GatewayError.DownstreamRetryable(response) => response

  private def errorResponse(error: String, message: String, status: Status): Response =
    Response.json(ErrorBody(error, message).toJson).status(status)
