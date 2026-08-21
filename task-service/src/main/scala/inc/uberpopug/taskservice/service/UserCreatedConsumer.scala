package inc.uberpopug.taskservice.service

import java.time.Instant

import zio.{Clock, Schedule, ZIO, ZLayer, durationInt}
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.Serde
import zio.metrics.Metric
import zio.stream.ZStream

import auth.user_created.UserCreated

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.PersistenceError
import inc.uberpopug.common.domain.UserId
import inc.uberpopug.taskservice.config.KafkaConfig
import inc.uberpopug.taskservice.domain.Role
import inc.uberpopug.taskservice.repository.ProcessedEventsRepository

/** Потребитель событий `UserCreated` из Auth Service: наполняет кэш попугов, доступных для назначения. */
trait UserCreatedConsumer:
  /** Бесконечный поток обработки событий (никогда не завершается). */
  def run: ZIO[Any, Nothing, Unit]

object UserCreatedConsumer:
  /** Слой Kafka-consumer'а группы `ates-task-service`, сконфигурированного из KafkaConfig. */
  val consumerLayer: ZLayer[KafkaConfig, Throwable, Consumer] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[KafkaConfig]
        consumer <- Consumer.make(
          ConsumerSettings(cfg.bootstrapServers).withGroupId(cfg.consumerGroupId)
        )
      yield consumer
    }

  /** Слой consumer-а: dependencies — Consumer, KafkaConfig, кэш попугов, репозиторий дедупликации и Clock. */
  val layer: ZLayer[
    Consumer & KafkaConfig & EligiblePopugs & ProcessedEventsRepository & Clock,
    Nothing,
    UserCreatedConsumer
  ] =
    ZLayer.fromFunction(UserCreatedConsumerLive(_, _, _, _, _))

/** Реализация consumer-а: подписка на `auth.user.created`, идемпотентная обработка через `processed_events`. */
final case class UserCreatedConsumerLive(
    consumer: Consumer,
    cfg: KafkaConfig,
    eligible: EligiblePopugs,
    processed: ProcessedEventsRepository,
    clock: Clock
) extends UserCreatedConsumer:

  /** Обрабатывает события: дедупликация, парсинг, добавление попуга в кэш. Бесконечный цикл: при фатальной ошибке
    * потока логирует её и переподписывается заново.
    */
  def run: ZIO[Any, Nothing, Unit] =
    ZIO.logInfo("UserCreatedConsumer started") *>
      (lagGaugeLoop.fork *>
        stream
          .mapZIO { committable =>
            handle(committable.record.value).foldZIO(
              error => ZIO.logError(s"Failed to process UserCreated ${committable.record.key}: ${describe(error)}"),
              _ => ZIO.unit
            ) *> ZIO.succeed(committable.offset)
          }
          .aggregateAsyncWithin(Consumer.collectOffsets, Schedule.fixed(100.millis))
          .mapZIO(_.commit)
          .runDrain
          .catchAll(error => ZIO.logError(s"UserCreatedConsumer stream failed: ${describe(error)}"))
          .forever)

  /** Бесконечный цикл публикации gauge `kafka_consumer_lag` из метрик Kafka-consumer (максимальный lag по партициям).
    */
  private def lagGaugeLoop: ZIO[Any, Nothing, Unit] =
    val publish =
      consumer.metrics
        .map(_.collectFirst {
          case (name, metric) if name.name() == "records-lag-max" =>
            Metric.gauge("kafka_consumer_lag").set(metric.metricValue().asInstanceOf[Number].doubleValue())
        })
        .flatMap(_.getOrElse(ZIO.unit))
        .catchAll(_ => ZIO.unit)
    (publish *> ZIO.sleep(10.seconds)).forever

  /** Поток записей топика `auth.user.created` (ключ — string, значение — protobuf-байты). */
  private val stream: ZStream[Any, Throwable, zio.kafka.consumer.CommittableRecord[String, Array[Byte]]] =
    consumer.plainStream(Subscription.topics(cfg.topicUserCreated), Serde.string, Serde.byteArray)

  private def handle(bytes: Array[Byte]): ZIO[Any, DomainError, Unit] =
    for
      now <- clock.instant
      _ <- UserCreatedProcessor.process(bytes, now, processed, eligible)
    yield ()

  /** Человекочитаемое описание ошибки для логов. */
  private def describe(error: Any): String =
    error match
      case domain: DomainError  => domain.toString
      case throwable: Throwable => Option(throwable.getMessage).getOrElse(throwable.toString)
      case other                => other.toString

/** Чистая обработка одного события `UserCreated`: дедупликация и наполнение кэша. Вынесена из consumer-а для
  * тестируемости без Kafka.
  */
object UserCreatedProcessor:
  /** Парсит protobuf-событие; уже обработанное событие пропускается; в кэш попадает только роль `popug`. */
  def process(
      bytes: Array[Byte],
      now: Instant,
      processed: ProcessedEventsRepository,
      eligible: EligiblePopugs
  ): ZIO[Any, DomainError, Unit] =
    for
      event <- ZIO
        .attempt(UserCreated.parseFrom(bytes))
        .mapError(ex => PersistenceError(s"Malformed UserCreated event: ${ex.getMessage}"))
      deduplicated <- processed.insertIfAbsent(java.util.UUID.fromString(event.eventId), "UserCreated", now)
      _ <-
        if !deduplicated then ZIO.unit
        else
          Role.from(event.role) match
            case Right(Role.Popug) => eligible.add(UserId(java.util.UUID.fromString(event.userId)))
            case Right(_)          => ZIO.unit
            case Left(error)       => ZIO.fail(error)
    yield ()
