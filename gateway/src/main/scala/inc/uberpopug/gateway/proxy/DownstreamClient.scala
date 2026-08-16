package inc.uberpopug.gateway.proxy

import zio.{Chunk, ZIO}
import zio.http.{Body, Client, Headers, Request, Response, URL}

import inc.uberpopug.gateway.config.ServicesConfig
import inc.uberpopug.gateway.security.Authenticated

/** Форвардит запросы клиента в downstream-сервисы через zio-http Client. Путь и query сохраняются, hop-by-hop заголовки
  * отбрасываются; верифицированная личность передаётся через доверенные `X-Auth-*` заголовки.
  */
final case class DownstreamClient(services: ServicesConfig):

  /** Retryable-статусы downstream: после исчерпания retry ответ пробрасывается клиенту как есть. */
  private val retryableStatusCodes: Set[Int] = Set(502, 503, 504)

  /** Заголовки, которые нельзя пробрасывать между хостами. */
  private val hopByHopHeaders: Set[String] =
    Set(
      "connection",
      "keep-alive",
      "proxy-authenticate",
      "proxy-authorization",
      "proxy-connection",
      "te",
      "trailer",
      "transfer-encoding",
      "upgrade",
      "host"
    )

  /** Identity-заголовки, которыми Gateway помечает верифицированную личность. Клиентские значения всегда вырезаются
    * (антиспуфинг) и заменяются данными из верифицированного JWT.
    */
  val identityHeaders: Set[String] = Set("x-auth-user-id", "x-auth-user-role")

  /** Base URL downstream-сервиса из конфига. */
  def baseUrl(downstream: Downstream): String = downstream match
    case Downstream.Auth        => services.auth.baseUrl
    case Downstream.TaskService => services.taskService.baseUrl
    case Downstream.Accounting  => services.accounting.baseUrl
    case Downstream.Analytics   => services.analytics.baseUrl

  /** Строит downstream-запрос: тот же метод, путь и query, проброс полезных заголовков и тела; identity-заголовки
    * заменяются на верифицированную личность.
    */
  def build(
      downstream: Downstream,
      request: Request,
      body: Chunk[Byte],
      identity: Option[Authenticated]
  ): Either[String, Request] =
    URL
      .decode(baseUrl(downstream))
      .left
      .map(_.getMessage)
      .map { base =>
        Request(
          method = request.method,
          url = base.addPath(request.url.path).addQueryParams(request.queryParameters),
          headers = forwardHeaders(request.headers, identity),
          body = Body.fromChunk(body)
        )
      }

  /** Выполняет запрос к downstream; транспортная ошибка или retryable 5xx — в error-канал GatewayError. */
  def call(request: Request): ZIO[Client, GatewayError, Response] =
    Client
      .batched(request)
      .mapError(ex => GatewayError.DownstreamUnreachable(ex.getMessage))
      .flatMap { response =>
        if retryableStatusCodes.contains(response.status.code) then ZIO.fail(GatewayError.DownstreamRetryable(response))
        else ZIO.succeed(response)
      }

  private def forwardHeaders(headers: Headers, identity: Option[Authenticated]): Headers =
    val sanitized = Headers.fromIterable(
      headers.iterator
        .filterNot(header =>
          hopByHopHeaders.contains(header.headerName.toLowerCase) || identityHeaders.contains(
            header.headerName.toLowerCase
          )
        )
        .toList
    )
    identity.fold(sanitized) { auth =>
      sanitized
        .addHeader(zio.http.Header.Custom("X-Auth-User-Id", auth.id.value.toString))
        .addHeader(zio.http.Header.Custom("X-Auth-User-Role", auth.role.wire))
    }
