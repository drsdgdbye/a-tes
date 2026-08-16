package inc.uberpopug.taskservice.domain

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Статус задачи. Переход только `open → completed`, обратный запрещён. */
enum TaskStatus(val wire: String):
  /** Задача открыта и ожидает выполнения. */
  case Open extends TaskStatus("open")

  /** Задача выполнена. */
  case Completed extends TaskStatus("completed")

object TaskStatus:
  /** Парсит статус из строки без учёта регистра; неизвестное значение — ошибка. */
  def from(value: String): Either[DomainError, TaskStatus] =
    TaskStatus.values
      .find(_.wire == value.trim.toLowerCase)
      .toRight(InvalidValue("status", s"Invalid status: '$value'"))
