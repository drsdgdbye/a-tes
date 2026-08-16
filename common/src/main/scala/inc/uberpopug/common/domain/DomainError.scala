package inc.uberpopug.common.domain

/** SSOT доменных ошибок в error-канале. Расширяется по мере реализации сервисов. */
enum DomainError:
  case InvalidValue(field: String, message: String)

  /** Auth Service. */
  case InvalidCredentials
  case UserNotFound(userId: String)
  case EmailAlreadyExists(email: String)
  case UserDisabled(userId: String)
  case SelfDisableForbidden
  case AccessDenied(message: String)
  case TokenInvalid(message: String)
  case RefreshTokenInvalid(message: String)
  case RegistrationDisabled
  case PersistenceError(message: String)

  /** Task Service. */
  case TaskNotFound(taskId: String)
  case BusinessRuleViolation(message: String)
  case OptimisticLockConflict(message: String)
