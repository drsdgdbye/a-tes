package inc.uberpopug.gateway.security

/** Причины отказа верификации access-токена. Все маппятся в `401 UNAUTHORIZED`. */
enum JwtVerificationError:
  case InvalidToken(message: String)
  case TokenExpired
  case InvalidIssuer
  case InvalidSubject
  case InvalidRole
