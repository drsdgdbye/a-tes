package inc.uberpopug.accounting.service

import java.time.ZoneOffset

import com.cronutils.model.Cron
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.CronType
import com.cronutils.parser.CronParser
import io.github.jkobejs.cron.syntax.*
import zio.{Duration, Schedule, ZIO, ZLayer}

import inc.uberpopug.accounting.config.AppConfig

/** Планировщик выплат: dual-mode. `useUtc = true` — QUARTZ-cron в UTC (первый тик — не сразу, эффект спит до следующего
  * срабатывания); `false` — фиксированный интервал.
  */
trait PayoutScheduler:
  /** Бесконечный цикл выплат (никогда не завершается). */
  def run: ZIO[Any, Nothing, Unit]

object PayoutScheduler:
  /** Слой планировщика поверх сервиса выплат, конфига и Clock. */
  val layer: ZLayer[PayoutService & AppConfig, Nothing, PayoutScheduler] =
    ZLayer.fromFunction(PayoutSchedulerLive(_, _))

/** Реализация планировщика. */
final case class PayoutSchedulerLive(payout: PayoutService, cfg: AppConfig) extends PayoutScheduler:
  /** Запускает бесконечный цикл выплат; ошибки отдельного прогона логируются и не роняют цикл. */
  def run: ZIO[Any, Nothing, Unit] =
    ZIO.logInfo(
      s"PayoutScheduler started: useUtc=${cfg.payout.useUtc}, cron='${cfg.payout.cronExpression}', interval=${cfg.payout.intervalSeconds}s"
    ) *>
      (if cfg.payout.useUtc then runUtc else runInterval)

  /** Режим cron (UTC): `repeatWithCron` сам спит до следующего срабатывания. */
  private def runUtc: ZIO[Any, Nothing, Unit] =
    payout.runOnce
      .catchAll(error => ZIO.logError(s"Payout run failed: $error"))
      .repeatWithCron(cron, ZoneOffset.UTC)
      .unit

  /** Режим интервала: фиксированная пауза между прогонами. */
  private def runInterval: ZIO[Any, Nothing, Unit] =
    payout.runOnce
      .catchAll(error => ZIO.logError(s"Payout run failed: $error"))
      .repeat(Schedule.fixed(Duration.fromSeconds(cfg.payout.intervalSeconds)))
      .unit

  /** QUARTZ-cron из конфига. */
  private def cron: Cron =
    new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ)).parse(cfg.payout.cronExpression)
