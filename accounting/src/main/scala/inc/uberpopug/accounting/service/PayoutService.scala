package inc.uberpopug.accounting.service

import java.time.ZoneOffset

import zio.{Clock, Ref, ZIO, ZLayer}

import inc.uberpopug.accounting.domain.{AccountEvent, BalanceCalculator, PayoutCalculator}
import inc.uberpopug.accounting.repository.EventStore
import inc.uberpopug.common.domain.{DomainError, Money, UserId}

/** Выплаты в конце дня: для каждого счёта `amount = max(0, balance)`, событие `AccountPayout` и `PaymentProcessed` в
  * outbox — атомарно. Детерминированный `event_id` делает повторный запуск за ту же дату идемпотентным.
  */
trait PayoutService:
  /** Один прогон выплат. */
  def runOnce: ZIO[Any, DomainError, Unit]

object PayoutService:
  /** Слой сервиса выплат поверх event store и Clock. */
  val layer: ZLayer[EventStore & Clock, Nothing, PayoutService] =
    ZLayer.fromFunction(PayoutServiceLive(_, _))

/** Реализация выплат: срез по `created_at` строго до начала расчёта (M-ACC-17). */
final case class PayoutServiceLive(store: EventStore, clock: Clock) extends PayoutService:
  /** Один прогон: балансы считаются проигрыванием событий до среза — события, пришедшие во время выплаты, уходят на
    * следующий день.
    */
  def runOnce: ZIO[Any, DomainError, Unit] =
    for
      now <- clock.instant
      date = now.atZone(ZoneOffset.UTC).toLocalDate
      events <- store.eventsBefore(now)
      users <- store.listUsers
      balances = events
        .groupBy(_.aggregateId)
        .view
        .mapValues(BalanceCalculator.currentBalance)
        .toMap
      paid <- Ref.make(0)
      _ <- ZIO.foreachDiscard(users) { user =>
        for
          balance = balances.getOrElse(user.userId, Money.zero)
          amount = PayoutCalculator.payoutAmount(balance)
          eventId = PayoutCalculator.payoutEventId(UserId(user.userId), date)
          applied <- store.appendWithOutbox(
            eventId,
            List(AccountEvent.AccountPayout(eventId, now, UserId(user.userId), amount, date)),
            List(PaymentProcessedBuilder.paymentProcessed(UserId(user.userId), user.name, amount, date, eventId, now))
          )
          _ <-
            if applied then paid.update(_ + 1)
            else ZIO.logInfo(s"Payout for ${user.userId} on $date already processed")
        yield ()
      }
      total <- paid.get
      _ <- ZIO.logInfo(s"Payout run for $date completed: $total accounts paid")
    yield ()
