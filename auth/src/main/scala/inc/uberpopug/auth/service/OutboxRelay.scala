package inc.uberpopug.auth.service

import org.apache.kafka.clients.producer.ProducerRecord
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.{Duration, ZIO, ZLayer}

import inc.uberpopug.auth.config.AppConfig
import inc.uberpopug.auth.repository.OutboxRepository
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
          ProducerSettings(cfg.kafka.bootstrapServers).withClientId("ates-auth-relay")
        )
      yield producer
    }

  /** Слой relay с зависимостями: outbox-репозиторий, producer и конфиг. */
  val layer: ZLayer[OutboxRepository & Producer & AppConfig, Nothing, OutboxRelay] =
    ZLayer.fromFunction { (repo: OutboxRepository, producer: Producer, cfg: AppConfig) =>
      OutboxRelayLive(repo, producer, cfg)
    }

/** Реализация relay: циклически публикует неопубликованные события в Kafka. */
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
          producer
            .produce(
              ProducerRecord(cfg.kafka.topicUserCreated, record.aggregateId.toString, record.payload),
              Serde.string,
              Serde.byteArray
            )
            .flatMap(_ => repo.markPublished(List(record.id)))
        publish.catchAll(error => ZIO.logError(s"Failed to publish outbox record ${record.id}: ${describe(error)}"))
      }
      _ <- sleepPolling
    yield ()).catchAll(error => ZIO.logError(s"OutboxRelay cycle failed: ${describe(error)}") *> sleepPolling)

  /** Человекочитаемое описание ошибки для логов. */
  private def describe(error: Any): String =
    error match
      case domain: DomainError  => domain.toString
      case throwable: Throwable => Option(throwable.getMessage).getOrElse(throwable.toString)
      case other                => other.toString

  /** Пауза между опросами outbox из конфига. */
  private def sleepPolling: ZIO[Any, Nothing, Unit] =
    ZIO.sleep(Duration.fromSeconds(cfg.outbox.pollIntervalSeconds))
