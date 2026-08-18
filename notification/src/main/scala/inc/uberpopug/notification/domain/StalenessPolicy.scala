package inc.uberpopug.notification.domain

import java.time.{Duration, Instant}

/** Защита от лавины уведомлений (спека §3.6): событие старше 5 минут не отправляется никому — оно потеряло
  * актуальность. Константа рядом с правилом (AGENTS §3).
  */
object StalenessPolicy:
  /** Максимальный возраст события для доставки. */
  val maxEventAge: Duration = Duration.ofMinutes(5)

  /** `true`, если событие старше 5 минут относительно `now`. */
  def isStale(eventTimestamp: Instant, now: Instant): Boolean =
    now.isAfter(eventTimestamp.plus(maxEventAge))
