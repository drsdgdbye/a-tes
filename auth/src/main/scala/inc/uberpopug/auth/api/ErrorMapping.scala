package inc.uberpopug.auth.api

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
      case InvalidValue(_, message)  => (StatusCode.BadRequest, ErrorResponse("VALIDATION_ERROR", message))
      case InvalidCredentials        => (StatusCode.Unauthorized, ErrorResponse("UNAUTHORIZED", "Invalid credentials"))
      case UserNotFound(userId)      => (StatusCode.NotFound, ErrorResponse("NOT_FOUND", s"User not found: $userId"))
      case EmailAlreadyExists(email) =>
        (StatusCode.Conflict, ErrorResponse("BUSINESS_RULE_VIOLATION", s"Email already exists: $email"))
      case UserDisabled(userId) => (StatusCode.Forbidden, ErrorResponse("FORBIDDEN", s"User is disabled: $userId"))
      case SelfDisableForbidden =>
        (StatusCode.Conflict, ErrorResponse("BUSINESS_RULE_VIOLATION", "Cannot disable your own account"))
      case AccessDenied(message)        => (StatusCode.Forbidden, ErrorResponse("FORBIDDEN", message))
      case TokenInvalid(message)        => (StatusCode.Unauthorized, ErrorResponse("UNAUTHORIZED", message))
      case RefreshTokenInvalid(message) => (StatusCode.Unauthorized, ErrorResponse("UNAUTHORIZED", message))
      case RegistrationDisabled    => (StatusCode.Forbidden, ErrorResponse("FORBIDDEN", "Registration is disabled"))
      case TaskNotFound(taskId)    => (StatusCode.NotFound, ErrorResponse("NOT_FOUND", s"Task not found: $taskId"))
      case AccountNotFound(userId) =>
        (StatusCode.NotFound, ErrorResponse("NOT_FOUND", s"Account not found: $userId"))
      case BusinessRuleViolation(message) =>
        (StatusCode.Conflict, ErrorResponse("BUSINESS_RULE_VIOLATION", message))
      case OptimisticLockConflict(message) =>
        (StatusCode.Conflict, ErrorResponse("CONFLICT", message))
      case PersistenceError(message)   => (StatusCode.InternalServerError, ErrorResponse("INTERNAL_ERROR", message))
      case TelegramSendFailed(message) =>
        (StatusCode.InternalServerError, ErrorResponse("INTERNAL_ERROR", message))
