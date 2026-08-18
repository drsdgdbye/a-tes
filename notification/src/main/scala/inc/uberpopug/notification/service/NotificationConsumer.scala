package inc.uberpopug.notification.service

import java.time.{Instant, LocalDate}
import java.util.UUID

import scala.util.Try

import com.google.protobuf.ByteString
import org.apache.kafka.clients.producer.ProducerRecord
import zio.{Clock, Schedule, ZIO, ZLayer, durationInt}
import zio.kafka.consumer.{CommittableRecord, Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.Producer
import zio.kafka.serde.Serde
import zio.stream.ZStream

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.{InvalidValue, TelegramSendFailed}
import inc.uberpopug.common.domain.UserId
import inc.uberpopug.notification.config.KafkaConfig
import inc.uberpopug.notification.domain.NotificationEvent

import accounting.payment_processed.PaymentProcessed
import internal.dead_letter_record.DeadLetterRecord
import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted

/** Потребитель событий TaskService/Accounting: маппит protobuf в доменные `NotificationEvent` и делегирует обработчику.
  * Poison pill (невалидные данные) → DLQ; ошибки доставки после внутренних retry → DLQ; транзиентные ошибки роняют
  * поток → переподписка с retry.
  */
trait NotificationConsumer:
  /** Бесконечный поток обработки событий (никогда не завершается). */
  def run: ZIO[Any, Nothing, Unit]

object NotificationConsumer:
  /** Слой Kafka-consumer'а группы `ates-notification`. */
  val consumerLayer: ZLayer[KafkaConfig, Throwable, Consumer] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[KafkaConfig]
        consumer <- Consumer.make(ConsumerSettings(cfg.bootstrapServers).withGroupId(cfg.consumerGroupId))
      yield consumer
    }

  /** Слой consumer-а: dependencies — Consumer, Producer (для DLQ), KafkaConfig, NotificationEventProcessor и Clock. */
  val layer
      : ZLayer[Consumer & Producer & KafkaConfig & NotificationEventProcessor & Clock, Nothing, NotificationConsumer] =
    ZLayer.fromFunction(NotificationConsumerLive(_, _, _, _, _))

  /** Ошибки, которые не исправить ретраем: невалидные данные и исчерпанные попытки доставки (retry внутри канала).
    * Транзиентные (PersistenceError) роняют поток → событие переобрабатывается после переподписки.
    */
  def isPoison(error: DomainError): Boolean =
    error match
      case InvalidValue(_, _) | TelegramSendFailed(_) => true
      case _                                          => false

/** Ошибка обработки, которая должна прервать поток (транзиентная), чтобы событие было переобработано после
  * переподписки.
  */
final case class ProcessingFailure(error: DomainError) extends RuntimeException(s"Transient processing failure: $error")

/** Реализация consumer-а: подписка на 3 топика, дедупликация через `processed_events`, DLQ для poison-pill и для
  * исчерпанных попыток доставки.
  */
final case class NotificationConsumerLive(
    consumer: Consumer,
    producer: Producer,
    cfg: KafkaConfig,
    processor: NotificationEventProcessor,
    clock: Clock
) extends NotificationConsumer:

  /** Бесконечный цикл: при фатальной ошибке потока логирует её и переподписывается заново. Перед каждой переподпиской —
    * bounded retry с экспоненциальным backoff.
    */
  def run: ZIO[Any, Nothing, Unit] =
    ZIO.logInfo("NotificationConsumer started") *>
      stream
        .mapZIO { committable =>
          handle(committable).foldZIO(
            error =>
              if NotificationConsumer.isPoison(error) then
                produceDlq(committable, error).foldZIO(
                  dlqError =>
                    ZIO.logError(
                      s"Failed to publish ${committable.record.topic()}/${committable.record.key()} to DLQ: ${describe(dlqError)}"
                    ) *> ZIO.fail(ProcessingFailure(error)),
                  _ =>
                    ZIO.logError(
                      s"Poison record ${committable.record.topic()}/${committable.record.key()} -> DLQ: ${describe(error)}"
                    ) *> ZIO.succeed(committable.offset)
                )
              else
                ZIO.logError(
                  s"Transient failure for ${committable.record.topic()}/${committable.record.key()}: ${describe(error)}"
                ) *> ZIO.fail(ProcessingFailure(error))
            ,
            _ => ZIO.succeed(committable.offset)
          )
        }
        .aggregateAsyncWithin(Consumer.collectOffsets, Schedule.fixed(100.millis))
        .mapZIO(_.commit)
        .runDrain
        .retry(Schedule.exponential(100.millis) && Schedule.recurs(4))
        .catchAll(error => ZIO.logError(s"NotificationConsumer stream failed after retries: ${describe(error)}"))
        .forever

  /** Поток записей трёх топиков (ключ — string, значение — protobuf-байты). */
  private val stream: ZStream[Any, Throwable, CommittableRecord[String, Array[Byte]]] =
    consumer.plainStream(
      Subscription.topics(
        cfg.topicTaskAssigned,
        cfg.topicTaskCompleted,
        cfg.topicPaymentProcessed
      ),
      Serde.string,
      Serde.byteArray
    )

  /** Диспетчеризация по топику: парсинг protobuf + маппинг в доменное событие + обработка. */
  private def handle(committable: CommittableRecord[String, Array[Byte]]): ZIO[Any, DomainError, Unit] =
    val bytes = committable.record.value()
    committable.record.topic() match
      case t if t == cfg.topicTaskAssigned =>
        parse(TaskAssigned.parseFrom)(bytes).flatMap(processTaskAssigned)
      case t if t == cfg.topicTaskCompleted =>
        parse(TaskCompleted.parseFrom)(bytes).flatMap(processTaskCompleted)
      case t if t == cfg.topicPaymentProcessed =>
        parse(PaymentProcessed.parseFrom)(bytes).flatMap(processPaymentProcessed)
      case other => ZIO.fail(InvalidValue("topic", s"Unexpected topic: $other"))

  /** `TaskAssigned` → уведомление новому исполнителю. */
  private def processTaskAssigned(event: TaskAssigned): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      popugId <- parseUserId(event.newAssigneeId)
      _ <- processor.process(
        eventId,
        "TaskAssigned",
        NotificationEvent.TaskAssigned(popugId, event.taskTitle, Instant.ofEpochMilli(event.timestamp))
      )
    yield ()

  /** `TaskCompleted` → уведомление исполнителю. */
  private def processTaskCompleted(event: TaskCompleted): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      popugId <- parseUserId(event.assigneeId)
      _ <- processor.process(
        eventId,
        "TaskCompleted",
        NotificationEvent.TaskCompleted(
          popugId,
          event.taskTitle,
          event.completeRewardCents,
          Instant.ofEpochMilli(event.timestamp)
        )
      )
    yield ()

  /** `PaymentProcessed` → уведомление о выплате. */
  private def processPaymentProcessed(event: PaymentProcessed): ZIO[Any, DomainError, Unit] =
    for
      eventId <- parseUuid(event.eventId)
      popugId <- parseUserId(event.popugId)
      date <- parseDate(event.date)
      _ <- processor.process(
        eventId,
        "PaymentProcessed",
        NotificationEvent.PaymentProcessed(popugId, event.amountCents, date, Instant.ofEpochMilli(event.timestamp))
      )
    yield ()

  /** Парсинг protobuf-байтов; повреждённые данные — `InvalidValue` (poison pill → DLQ). */
  private def parse[A](parser: Array[Byte] => A)(bytes: Array[Byte]): ZIO[Any, DomainError, A] =
    ZIO
      .attempt(parser(bytes))
      .mapError(ex => InvalidValue("event", s"Malformed event: ${Option(ex.getMessage).getOrElse(ex.toString)}"))

  /** Безопасный парсинг UUID: невалидное значение — `InvalidValue`. */
  private def parseUuid(value: String): ZIO[Any, DomainError, UUID] =
    ZIO.fromEither(
      Try(UUID.fromString(value)).toEither.left.map(_ => InvalidValue("event", s"Invalid UUID: '$value'"))
    )

  /** Безопасный парсинг userId: невалидное значение — `InvalidValue`. */
  private def parseUserId(value: String): ZIO[Any, DomainError, UserId] =
    ZIO.fromEither(UserId.from(value))

  /** Безопасный парсинг ISO-даты: невалидное значение — `InvalidValue`. */
  private def parseDate(value: String): ZIO[Any, DomainError, LocalDate] =
    ZIO
      .attempt(LocalDate.parse(value))
      .mapError(_ => InvalidValue("date", s"Invalid ISO-8601 date: '$value'"))

  /** Публикует необрабатываемое событие в DLQ как `DeadLetterRecord`. */
  private def produceDlq(
      committable: CommittableRecord[String, Array[Byte]],
      error: DomainError
  ): ZIO[Any, Throwable, Unit] =
    for
      now <- clock.instant
      record = DeadLetterRecord(
        originalTopic = committable.record.topic(),
        originalValue = ByteString.copyFrom(committable.record.value()),
        errorMessage = describe(error),
        failedAt = now.toEpochMilli,
        originalOffset = committable.offset.offset,
        partition = committable.offset.partition
      )
      _ <- producer.produce(ProducerRecord(cfg.topicDlq, null, record.toByteArray), Serde.string, Serde.byteArray)
    yield ()

  /** Человекочитаемое описание ошибки для логов. */
  private def describe(error: Any): String =
    error match
      case domain: DomainError  => domain.toString
      case throwable: Throwable => Option(throwable.getMessage).getOrElse(throwable.toString)
      case other                => other.toString
