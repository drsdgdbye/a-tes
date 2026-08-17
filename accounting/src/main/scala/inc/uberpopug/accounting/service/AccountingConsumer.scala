package inc.uberpopug.accounting.service

import com.google.protobuf.ByteString
import org.apache.kafka.clients.producer.ProducerRecord
import zio.{Clock, Schedule, ZIO, ZLayer, durationInt}
import zio.kafka.consumer.{CommittableRecord, Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.Producer
import zio.kafka.serde.Serde
import zio.stream.ZStream

import inc.uberpopug.accounting.config.KafkaConfig
import inc.uberpopug.accounting.repository.EventStore
import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.{BusinessRuleViolation, InvalidValue}

import auth.user_created.UserCreated
import internal.dead_letter_record.DeadLetterRecord
import task.task_assigned.TaskAssigned
import task.task_completed.TaskCompleted
import task.task_created.TaskCreated

/** Потребитель событий TaskService/Auth: наполняет event store, проекцию пользователей и балансы. Poison pill
  * (невалидный protobuf/данные, нарушение бизнес-правила) → DLQ; транзиентные ошибки (включая событие для ещё не
  * существующего счёта) роняют поток → переподписка с retry.
  */
trait AccountingConsumer:
  /** Бесконечный поток обработки событий (никогда не завершается). */
  def run: ZIO[Any, Nothing, Unit]

object AccountingConsumer:
  /** Слой Kafka-consumer'а группы `ates-accounting`. */
  val consumerLayer: ZLayer[KafkaConfig, Throwable, Consumer] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[KafkaConfig]
        consumer <- Consumer.make(ConsumerSettings(cfg.bootstrapServers).withGroupId(cfg.consumerGroupId))
      yield consumer
    }

  /** Слой consumer-а: dependencies — Consumer, Producer (для DLQ), KafkaConfig, EventStore и Clock. */
  val layer: ZLayer[Consumer & Producer & KafkaConfig & EventStore & Clock, Nothing, AccountingConsumer] =
    ZLayer.fromFunction(AccountingConsumerLive(_, _, _, _, _))

/** Ошибка обработки, которая должна прервать поток (транзиентная), чтобы событие было переобработано после
  * переподписки.
  */
final case class ProcessingFailure(error: DomainError) extends RuntimeException(s"Transient processing failure: $error")

/** Реализация consumer-а: подписка на 4 топика, дедупликация через event store, DLQ для poison-pill. */
final case class AccountingConsumerLive(
    consumer: Consumer,
    producer: Producer,
    cfg: KafkaConfig,
    store: EventStore,
    clock: Clock
) extends AccountingConsumer:

  /** Бесконечный цикл: при фатальной ошибке потока логирует её и переподписывается заново. Перед каждой переподпиской —
    * bounded retry с экспоненциальным backoff (не даёт busy-loop и даёт `auth.user.created` время догнать при
    * catch-up).
    */
  def run: ZIO[Any, Nothing, Unit] =
    ZIO.logInfo("AccountingConsumer started") *>
      stream
        .mapZIO { committable =>
          handle(committable).foldZIO(
            error =>
              if isPoison(error) then
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
        .catchAll(error => ZIO.logError(s"AccountingConsumer stream failed after retries: ${describe(error)}"))
        .forever

  /** Поток записей четырёх топиков (ключ — string, значение — protobuf-байты). */
  private val stream: ZStream[Any, Throwable, CommittableRecord[String, Array[Byte]]] =
    consumer.plainStream(
      Subscription.topics(cfg.topicUserCreated, cfg.topicTaskCreated, cfg.topicTaskAssigned, cfg.topicTaskCompleted),
      Serde.string,
      Serde.byteArray
    )

  /** Диспетчеризация по топику: парсинг protobuf + обработка. */
  private def handle(committable: CommittableRecord[String, Array[Byte]]): ZIO[Any, DomainError, Unit] =
    val bytes = committable.record.value()
    committable.record.topic() match
      case t if t == cfg.topicUserCreated =>
        parse(UserCreated.parseFrom)(bytes).flatMap(AccountingEventProcessor.processUserCreated(_, store))
      case t if t == cfg.topicTaskCreated =>
        parse(TaskCreated.parseFrom)(bytes).flatMap(AccountingEventProcessor.processTaskCreated(_, store))
      case t if t == cfg.topicTaskAssigned =>
        parse(TaskAssigned.parseFrom)(bytes).flatMap(AccountingEventProcessor.processTaskAssigned(_, store))
      case t if t == cfg.topicTaskCompleted =>
        parse(TaskCompleted.parseFrom)(bytes).flatMap(AccountingEventProcessor.processTaskCompleted(_, store))
      case other => ZIO.fail(InvalidValue("topic", s"Unexpected topic: $other"))

  /** Парсинг protobuf-байтов; повреждённые данные — `InvalidValue` (poison pill → DLQ). */
  private def parse[A](parser: Array[Byte] => A)(bytes: Array[Byte]): ZIO[Any, DomainError, A] =
    ZIO
      .attempt(parser(bytes))
      .mapError(ex => InvalidValue("event", s"Malformed event: ${Option(ex.getMessage).getOrElse(ex.toString)}"))

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

  /** Ошибки данных, которые не исправить ретраем: только они уходят в DLQ. `AccountNotFound` сюда НЕ входит: при
    * восстановлении `auth.user.created` приходит позже (catch-up), поэтому событие транзиентно и переобрабатывается.
    */
  private def isPoison(error: DomainError): Boolean =
    error match
      case InvalidValue(_, _) | BusinessRuleViolation(_) => true
      case _                                             => false

  /** Человекочитаемое описание ошибки для логов. */
  private def describe(error: Any): String =
    error match
      case domain: DomainError  => domain.toString
      case throwable: Throwable => Option(throwable.getMessage).getOrElse(throwable.toString)
      case other                => other.toString
