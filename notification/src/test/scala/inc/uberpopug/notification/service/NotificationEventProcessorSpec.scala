package inc.uberpopug.notification.service

import java.time.Instant
import java.util.UUID

import zio.{Ref, ZIO}
import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.common.domain.DomainError
import inc.uberpopug.common.domain.DomainError.TelegramSendFailed
import inc.uberpopug.common.domain.UserId
import inc.uberpopug.notification.{InMemoryNotificationState, InMemoryNotificationStore, orDieE}
import inc.uberpopug.notification.domain.{ChannelType, NotificationEvent, NotificationTextBuilder, RenderedMessage}
import inc.uberpopug.notification.repository.NotificationStore

/** Тесты пайплайна обработки события (M-NTF-01..07): доставка по типам событий, уведомление админам, защита от лавины и
  * дедупликация. Kafka/Telegram/Postgres не задействуются: store и канал — in-memory, время — TestClock.
  */
object NotificationEventProcessorSpec extends ZIOSpecDefault:

  private val popug = UserId(UUID.randomUUID())
  private val popugAddress = "chat-popug-1"
  private val ts = Instant.ofEpochMilli(0L)

  /** Записывает отправленные сообщения в Ref (имитация реального канала). */
  final case class RecordingChannel(sent: Ref[List[RenderedMessage]]) extends NotificationChannel:
    val channelType: ChannelType = ChannelType.Telegram
    def send(message: RenderedMessage): ZIO[Any, DomainError, Unit] =
      sent.update(_ :+ message)

  /** Канал, который всегда падает — проверка проброса ошибки доставки. */
  final case class FailingChannel() extends NotificationChannel:
    val channelType: ChannelType = ChannelType.Telegram
    def send(message: RenderedMessage): ZIO[Any, DomainError, Unit] =
      ZIO.fail(TelegramSendFailed("telegram down"))

  private def makeStandard(
      adminAddresses: List[String] = Nil,
      withContact: Boolean = true
  ): ZIO[
    Any,
    Nothing,
    (NotificationEventProcessor, NotificationStore, Ref[List[RenderedMessage]], Ref[InMemoryNotificationState])
  ] =
    for
      state <- Ref.make(InMemoryNotificationState())
      store = InMemoryNotificationStore(state)
      sent <- Ref.make(List.empty[RenderedMessage])
      _ <-
        if withContact then state.update(_.copy(contacts = Map((popug.value, "telegram") -> popugAddress)))
        else ZIO.unit
      clock <- ZIO.clock
      registry = ChannelRegistry(
        Map(ChannelType.Telegram -> ChannelEntry(RecordingChannel(sent), adminAddresses))
      )
      processor = NotificationEventProcessorLive(store, registry, clock)
    yield (processor, store, sent, state)

  private val assigned = NotificationEvent.TaskAssigned(popug, "Fix bug", ts)
  private val completed = NotificationEvent.TaskCompleted(popug, "Fix bug", 1234L, ts)
  private val payment = NotificationEvent.PaymentProcessed(popug, 259999L, java.time.LocalDate.of(2026, 1, 15), ts)

  def spec: Spec[Any, Any] =
    suite("NotificationEventProcessor")(
      suite("M-NTF-01..03 delivery by event type")(
        test("M-NTF-01 TaskAssigned -> сообщение новому исполнителю") {
          for
            (processor, _, sent, _) <- makeStandard()
            _ <- processor.process(UUID.randomUUID(), "TaskAssigned", assigned).orDieE
            messages <- sent.get
          yield assertTrue(messages.length == 1) &&
            assertTrue(messages.head.address == popugAddress) &&
            assertTrue(messages.head.text == NotificationTextBuilder.render(assigned))
        },
        test("M-NTF-02 TaskCompleted -> сообщение исполнителю с суммой") {
          for
            (processor, _, sent, _) <- makeStandard()
            _ <- processor.process(UUID.randomUUID(), "TaskCompleted", completed).orDieE
            messages <- sent.get
          yield assertTrue(messages.length == 1) &&
            assertTrue(messages.head.address == popugAddress) &&
            assertTrue(messages.head.text == NotificationTextBuilder.render(completed))
        },
        test("M-NTF-03 PaymentProcessed -> «Выплата за DD.MM: $XX»") {
          for
            (processor, _, sent, _) <- makeStandard()
            _ <- processor.process(UUID.randomUUID(), "PaymentProcessed", payment).orDieE
            messages <- sent.get
          yield assertTrue(messages.length == 1) &&
            assertTrue(messages.head.address == popugAddress) &&
            assertTrue(messages.head.text == "Выплата за 15.01: $2599.99")
        }
      ),
      suite("M-NTF-04 missing mapping -> admin notification")(
        test("нет контакта попуга -> сообщение на все админские адреса канала") {
          for
            (processor, _, sent, _) <- makeStandard(adminAddresses = List("admin-1", "admin-2"), withContact = false)
            _ <- processor.process(UUID.randomUUID(), "TaskAssigned", assigned).orDieE
            messages <- sent.get
          yield assertTrue(messages.length == 2) &&
            assertTrue(messages.map(_.address).toSet == Set("admin-1", "admin-2")) &&
            assertTrue(
              messages.forall(_.text.contains(s"Не удалось доставить уведомление (TaskAssigned) попугу ${popug.value}"))
            )
        },
        test("нет контакта и нет админских адресов -> ничего, без ошибки") {
          for
            (processor, _, sent, _) <- makeStandard(adminAddresses = Nil, withContact = false)
            _ <- processor.process(UUID.randomUUID(), "TaskAssigned", assigned).orDieE
            messages <- sent.get
          yield assertTrue(messages.isEmpty)
        }
      ),
      suite("M-NTF-05/06 staleness (lavina)")(
        test("M-NTF-05 событие старше 5 минут -> не отправляется никому, но помечается обработанным") {
          for
            (processor, _, sent, state) <- makeStandard()
            eventId = UUID.randomUUID()
            _ <- TestClock.adjust(java.time.Duration.ofMinutes(6))
            _ <- processor.process(eventId, "TaskAssigned", assigned).orDieE
            messages <- sent.get
            processed <- state.get.map(_.processed)
          yield assertTrue(messages.isEmpty) && assertTrue(processed.contains(eventId))
        },
        test("M-NTF-06 событие старше 5 минут и нет маппинга -> не слать ни админу, ни попугу") {
          for
            (processor, _, sent, _) <- makeStandard(adminAddresses = List("admin-1"), withContact = false)
            _ <- TestClock.adjust(java.time.Duration.ofMinutes(6))
            _ <- processor.process(UUID.randomUUID(), "TaskAssigned", assigned).orDieE
            messages <- sent.get
          yield assertTrue(messages.isEmpty)
        },
        test("свежее событие (возраст 0) -> доставляется") {
          for
            (processor, _, sent, _) <- makeStandard()
            _ <- processor.process(UUID.randomUUID(), "TaskAssigned", assigned).orDieE
            messages <- sent.get
          yield assertTrue(messages.length == 1)
        }
      ),
      suite("M-NTF-07 deduplication")(
        test("повторный process того же event_id -> второе сообщение не отправляется") {
          for
            (processor, _, sent, _) <- makeStandard()
            eventId = UUID.randomUUID()
            _ <- processor.process(eventId, "TaskAssigned", assigned).orDieE
            _ <- processor.process(eventId, "TaskAssigned", assigned).orDieE
            messages <- sent.get
          yield assertTrue(messages.length == 1)
        },
        test("событие уже в sent_notifications(event_id, channel, address) -> send не вызывается") {
          for
            (processor, _, sent, state) <- makeStandard()
            eventId = UUID.randomUUID()
            _ <- state.update(_.copy(sent = Set((eventId, "telegram", popugAddress))))
            _ <- processor.process(eventId, "TaskAssigned", assigned).orDieE
            messages <- sent.get
          yield assertTrue(messages.isEmpty)
        }
      ),
      suite("error propagation")(
        test("ошибка канала (TelegramSendFailed) пробрасывается из process") {
          for
            (processor, _, _, _) <- makeStandardWithChannel(adminAddresses = Nil, channel = FailingChannel())
            result <- processor.process(UUID.randomUUID(), "TaskAssigned", assigned).either
          yield assert(result)(isLeft(isSubtype[TelegramSendFailed](anything)))
        }
      )
    )

  private def makeStandardWithChannel(
      adminAddresses: List[String],
      channel: NotificationChannel
  ): ZIO[
    Any,
    Nothing,
    (
        NotificationEventProcessor,
        NotificationStore,
        Ref[List[RenderedMessage]],
        Ref[
          InMemoryNotificationState
        ]
    )
  ] =
    for
      state <- Ref.make(InMemoryNotificationState())
      store = InMemoryNotificationStore(state)
      sent <- Ref.make(List.empty[RenderedMessage])
      _ <- state.update(_.copy(contacts = Map((popug.value, "telegram") -> popugAddress)))
      clock <- ZIO.clock
      registry = ChannelRegistry(Map(ChannelType.Telegram -> ChannelEntry(channel, adminAddresses)))
      processor = NotificationEventProcessorLive(store, registry, clock)
    yield (processor, store, sent, state)
