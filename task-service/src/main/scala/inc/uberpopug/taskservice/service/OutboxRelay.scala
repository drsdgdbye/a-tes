package inc.uberpopug.taskservice.service

import org.apache.kafka.clients.producer.ProducerRecord
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.{Duration, ZIO, ZLayer}

import inc.uberpopug.taskservice.config.AppConfig
import inc.uberpopug.taskservice.repository.OutboxRepository
import inc.uberpopug.common.domain.DomainError

/** Публикация событий из transactional outbox в Kafka (фоновый цикл). */
trait OutboxRelay:
  /** Бесконечный цикл: забирает батч outbox, публикует в Kafka, помечает published. */
  def run: ZIO[Any, Nothing, Unit]

object OutboxRelay:
  /** Слой Kafka-producer'а, сконфигурированного из AppConfig. */
  val producerLayer: ZLayer[AppConfig, Throwable, Producer] =
    ZLayer.scoped {
      for
        cfg <- ZIO.service[AppConfig]
        producer <- Producer.make(
          ProducerSettings(cfg.kafka.bootstrapServers).withClientId("ates-task-service-relay")
        )
      yield producer
    }

  /** Слой relay с зависимостями: outbox-репозиторий, producer и конфиг. */
  val layer: ZLayer[OutboxRepository & Producer & AppConfig, Nothing, OutboxRelay] =
    ZLayer.fromFunction { (repo: OutboxRepository, producer: Producer, cfg: AppConfig) =>
      OutboxRelayLive(repo, producer, cfg)
    }

/** Реализация relay: циклически публикует неопубликованные события в Kafka. Топик определяется типом события. */
final case class OutboxRelayLive(
    repo: OutboxRepository,
    producer: Producer,
    cfg: AppConfig
) extends OutboxRelay:

  /** Запускает бесконечный цикл публикации (никогда не завершается). */
  def run: ZIO[Any, Nothing, Unit] =
    (for
      _ <- ZIO.logInfo("OutboxRelay started")
      _ <- loop
    yield ()).forever

  /** Одна итерация: claim батча, публикация каждого события с пометкой published (ошибки отдельных записей логируются и
    * не роняют цикл), затем пауза до следующего опроса.
    */
  private val loop: ZIO[Any, Nothing, Unit] =
    (for
      records <- repo.claimBatch(cfg.outbox.batchSize)
      _ <- ZIO.foreachDiscard(records) { record =>
        val publish =
          for
            topic <- ZIO.fromOption(topicFor(record.eventType))
            _ <- producer.produce(
              ProducerRecord(topic, record.aggregateId.toString, record.payload),
              Serde.string,
              Serde.byteArray
            )
            _ <- repo.markPublished(List(record.id))
          yield ()
        publish.catchAll(error =>
          ZIO.logError(s"Failed to publish outbox record ${record.id} (${record.eventType}): ${describe(error)}")
        )
      }
      _ <- sleepPolling
    yield ()).catchAll(error => ZIO.logError(s"OutboxRelay cycle failed: ${describe(error)}") *> sleepPolling)

  /** Маппит тип события на топик Kafka; неизвестный тип — `None` (запись остаётся в outbox и логируется). */
  private def topicFor(eventType: String): Option[String] =
    eventType match
      case TaskEventTypes.TaskCreated   => Some(cfg.kafka.topicTaskCreated)
      case TaskEventTypes.TaskAssigned  => Some(cfg.kafka.topicTaskAssigned)
      case TaskEventTypes.TaskCompleted => Some(cfg.kafka.topicTaskCompleted)
      case _                            => None

  /** Человекочитаемое описание ошибки для логов. */
  private def describe(error: Any): String =
    error match
      case domain: DomainError  => domain.toString
      case throwable: Throwable => Option(throwable.getMessage).getOrElse(throwable.toString)
      case other                => other.toString

  /** Пауза между опросами outbox из конфига. */
  private def sleepPolling: ZIO[Any, Nothing, Unit] =
    ZIO.sleep(Duration.fromSeconds(cfg.outbox.pollIntervalSeconds))
