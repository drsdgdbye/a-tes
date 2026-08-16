package inc.uberpopug.taskservice.api

import sttp.model.StatusCode

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.*

/** Единый формат тела ошибки API: код + человекочитаемое сообщение. */
final case class ErrorResponse(error: String, message: String)

object ErrorMapping:
  /** Маппит доменную ошибку в HTTP-статус и тело ответа. Единственное место, где DomainError превращается в
    * HTTP-контракт (SSOT маппинга ошибок).
    */
  def toApiError(error: DomainError): (StatusCode, ErrorResponse) =
    error match
      case InvalidValue(_, message) => (StatusCode.BadRequest, ErrorResponse("VALIDATION_ERROR", message))
      case TaskNotFound(taskId)     => (StatusCode.NotFound, ErrorResponse("NOT_FOUND", s"Task not found: $taskId"))
      case BusinessRuleViolation(message) =>
        (StatusCode.Conflict, ErrorResponse("BUSINESS_RULE_VIOLATION", message))
      case OptimisticLockConflict(message) =>
        (StatusCode.Conflict, ErrorResponse("CONFLICT", message))
      case AccessDenied(message)     => (StatusCode.Forbidden, ErrorResponse("FORBIDDEN", message))
      case PersistenceError(message) =>
        (StatusCode.InternalServerError, ErrorResponse("INTERNAL_ERROR", message))
      case other => (StatusCode.InternalServerError, ErrorResponse("INTERNAL_ERROR", other.toString))
