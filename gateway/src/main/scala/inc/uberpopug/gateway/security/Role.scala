package inc.uberpopug.gateway.security

/** Роли пользователей: wire-значение из JWT-claims. Дублирует значения Auth для валидации на Gateway; бизнес-доступ
  * проверяется downstream-сервисами.
  */
enum Role(val wire: String):
  case Popug extends Role("popug")
  case Manager extends Role("manager")
  case Accountant extends Role("accountant")
  case Admin extends Role("admin")

object Role:
  /** Парсит wire-значение роли; неизвестное значение — `Left`. */
  def from(value: String): Either[String, Role] =
    Role.values.find(_.wire == value).toRight(s"Unknown role: '$value'")
