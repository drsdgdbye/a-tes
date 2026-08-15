package inc.uberpopug.gateway.proxy

/** Целевой downstream-сервис для маршрута. */
enum Downstream:
  case Auth
  case TaskService
  case Accounting
  case Analytics

object RouteResolver:
  /** Публичные пути Auth: проксируются без JWT-проверки на Gateway (Auth сам их обслуживает). */
  private val publicAuthPaths: Set[String] =
    Set("/auth/login", "/auth/register", "/auth/config", "/auth/refresh", "/auth/logout", "/auth/keys")

  /** Определяет downstream-сервис по пути запроса; `None` — маршрут не известен (404). */
  def resolve(path: String): Option[Downstream] =
    if path.startsWith("/auth/") || path.startsWith("/users") then Some(Downstream.Auth)
    else if path.startsWith("/tasks") then Some(Downstream.TaskService)
    else if path.startsWith("/accounts") then Some(Downstream.Accounting)
    else if path.startsWith("/analytics") then Some(Downstream.Analytics)
    else None

  /** Публичный ли путь (не требует JWT на Gateway). */
  def isPublic(path: String): Boolean = publicAuthPaths.contains(path)
