package inc.uberpopug.e2e

import zio.*
import zio.http.*
import zio.json.*

/** Упрощённый HTTP-клиент для E2E-запросов к Gateway через zio-http. */
final case class GatewayClient(baseUrl: String, client: Client):
  private def uri(path: String): URL = URL.decode(s"$baseUrl$path").toOption.get

  private def method(name: String): Method = if name == "POST" then Method.POST else Method.GET

  def postJson[A: JsonEncoder, R: JsonDecoder](
      path: String,
      body: A,
      token: Option[String] = None
  ): ZIO[Scope, Throwable, (Int, R)] =
    request("POST", path, body.toJson.getBytes, token)

  def getJson[R: JsonDecoder](path: String, token: Option[String] = None): ZIO[Scope, Throwable, (Int, R)] =
    request("GET", path, Array.emptyByteArray, token)

  private def raw(methodName: String, path: String, bodyBytes: Array[Byte], token: Option[String]): ZIO[Scope, Throwable, (Int, String)] =
    var headers = Headers(Header.ContentType(MediaType.application.json))
    token.foreach(t => headers = headers.addHeader(Header.Authorization.Bearer(t)))
    val req =
      Request(method = method(methodName), url = uri(path), headers = headers, body = Body.fromArray(bodyBytes))
    client.batched(req).flatMap { res =>
      res.body.asString.map(s => (res.status.code, s))
    }

  private def request[R: JsonDecoder](
      methodName: String,
      path: String,
      bodyBytes: Array[Byte],
      token: Option[String]
  ): ZIO[Scope, Throwable, (Int, R)] =
    raw(methodName, path, bodyBytes, token).flatMap { case (code, s) =>
      ZIO
        .fromEither(s.fromJson[R])
        .map(r => (code, r))
        .mapError(e => new RuntimeException(s"JSON decode failed for $path: $e, body=$s"))
    }
