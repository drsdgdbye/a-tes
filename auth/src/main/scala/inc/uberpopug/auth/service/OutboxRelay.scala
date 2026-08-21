package inc.uberpopug.auth.service

import org.apache.kafka.clients.producer.ProducerRecord
import zio.kafka.producer.{Producer, ProducerSettings}
import zio.kafka.serde.Serde
import zio.{Duration, ZIO, ZLayer}

import inc.uberpopug.auth.config.AppConfig
import inc.uberpopug.auth.repository.OutboxRepository
import inc.uberpopug.common.domain.DomainError

/** Публикация события `UserCreated` из transactional outbox в Kafka. Вынесена в отдельный трейт, чтобы relay можно было
  * тестировать на in-memory реализациях без настоящего Kafka-брокера.
  */
trait OutboxPublisher:
  /** Публикует событие в Kafka по ключу (id агрегата) и payload. */
  def publish(key: String, value: Array[Byte]): ZIO[Any, Throwable, Unit]

object OutboxPublisher:
  /** Адаптер поверх zio-kafka Producer: тема берётся из конфига при построении слоя. */
  val layer: ZLayer[Producer & AppConfig, Nothing, OutboxPublisher] =
    ZLayer.fromFunction { (producer: Producer, cfg: AppConfig) =>
      new OutboxPublisher:
        def publish(key: String, value: Array[Byte]): ZIO[Any, Throwable, Unit] =
          producer
            .produce(ProducerRecord(cfg.kafka.topicUserCreated, key, value), Serde.string, Serde.byteArray)
            .unit
    }

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

  /** Слой relay с зависимостями: outbox-репозиторий, publisher и конфиг. */
  val layer: ZLayer[OutboxRepository & OutboxPublisher & AppConfig, Nothing, OutboxRelay] =
    ZLayer.fromFunction { (repo: OutboxRepository, publisher: OutboxPublisher, cfg: AppConfig) =>
      OutboxRelayLive(repo, publisher, cfg)
    }

/** Реализация relay: циклически публикует неопубликованные события в Kafka. */
final case class OutboxRelayLive(
    repo: OutboxRepository,
    publisher: OutboxPublisher,
    cfg: AppConfig
) extends OutboxRelay:

  /** Запускает бесконечный цикл публикации (никогда не завершается). */
  def run: ZIO[Any, Nothing, Unit] =
    (for
      _ <- ZIO.logInfo("OutboxRelay started")
      _ <- processBatch
      _ <- sleepPolling
    yield ()).forever

  /** Один цикл: claim батча, публикация каждого события с пометкой published (ошибки отдельных записей логируются и не
    * роняют цикл). Открыт для тестов в этом пакете.
    */
  private[service] def processBatch: ZIO[Any, Nothing, Unit] =
    (for
      records <- repo.claimBatch(cfg.outbox.batchSize)
      _ <- ZIO.foreachDiscard(records) { record =>
        publisher
          .publish(record.aggregateId.toString, record.payload)
          .flatMap(_ => repo.markPublished(List(record.id)))
          .catchAll(error => ZIO.logError(s"Failed to publish outbox record ${record.id}: ${describe(error)}"))
      }
    yield ()).catchAll(error => ZIO.logError(s"OutboxRelay cycle failed: ${describe(error)}"))

  /** Человекочитаемое описание ошибки для логов. */
  private def describe(error: Any): String =
    error match
      case domain: DomainError  => domain.toString
      case throwable: Throwable => Option(throwable.getMessage).getOrElse(throwable.toString)
      case other                => other.toString

  /** Пауза между опросами outbox из конфига. */
  private def sleepPolling: ZIO[Any, Nothing, Unit] =
    ZIO.sleep(Duration.fromSeconds(cfg.outbox.pollIntervalSeconds))
