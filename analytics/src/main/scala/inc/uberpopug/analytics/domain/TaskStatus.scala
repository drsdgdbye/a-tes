package inc.uberpopug.analytics.domain

/** Статус задачи в read-side проекции `tasks`. Переход только `open → completed`. */
enum TaskStatus(val wire: String):
  case Open extends TaskStatus("open")
  case Completed extends TaskStatus("completed")
