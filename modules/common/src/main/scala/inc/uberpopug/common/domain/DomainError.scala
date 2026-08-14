package inc.uberpopug.common.domain

/** SSOT доменных ошибок в error-канале. Расширяется по мере реализации сервисов. */
enum DomainError:
  case InvalidValue(field: String, message: String)
