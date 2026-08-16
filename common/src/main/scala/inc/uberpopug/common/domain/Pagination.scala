package inc.uberpopug.common.domain

/** Общий helper пагинации: нормализует опциональные query-параметры `limit`/`offset` к фиксированным границам. */
final case class Pagination(limit: Int, offset: Int)

object Pagination:
  /** Значение limit по умолчанию, если параметр не передан. */
  val DefaultLimit = 50

  /** Верхняя граница limit: предохраняет от запроса слишком больших страниц. */
  val MaxLimit = 100

  /** Нормализует `limit`/`offset` из API: неположительные или отсутствующие значения заменяются дефолтами, `limit`
    * ограничивается сверху.
    */
  def from(limit: Option[Int], offset: Option[Int]): Pagination =
    val normalizedLimit = limit.filter(_ > 0).getOrElse(DefaultLimit).min(MaxLimit)
    val normalizedOffset = offset.filter(_ > 0).getOrElse(0)
    Pagination(normalizedLimit, normalizedOffset)
