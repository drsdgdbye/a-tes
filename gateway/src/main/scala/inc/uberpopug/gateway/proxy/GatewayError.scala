package inc.uberpopug.gateway.proxy

import zio.http.Response

/** Ошибки проксирования Gateway. Возникают на транспортном уровне (downstream недоступен, таймаут) либо при
  * retryable-ответе downstream после исчерпания retry.
  */
enum GatewayError:
  /** Downstream недоступен (connection refused, circuit breaker open, bulkhead full) → `503`. */
  case DownstreamUnreachable(message: String)

  /** Превышен таймаут запроса к downstream → `504`. */
  case CallTimedOut

  /** Downstream вернул retryable 5xx; после исчерпания retry ответ пробрасывается клиенту как есть. */
  case DownstreamRetryable(response: Response)
