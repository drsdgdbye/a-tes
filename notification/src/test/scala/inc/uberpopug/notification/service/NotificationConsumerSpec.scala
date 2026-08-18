package inc.uberpopug.notification.service

import zio.test.*

import inc.uberpopug.common.domain.DomainError.*

/** Тесты классификации ошибок consumer-а (суть M-NTF-08): poison pill (невалидные данные, исчерпанные попытки доставки)
  * → DLQ; транзиентные → переподписка и переобработка.
  */
object NotificationConsumerSpec extends ZIOSpecDefault:

  def spec: Spec[Any, Any] =
    suite("NotificationConsumer.isPoison")(
      test("InvalidValue (битые данные) -> DLQ") {
        assertTrue(NotificationConsumer.isPoison(InvalidValue("event", "Malformed event")))
      },
      test("TelegramSendFailed (исчерпаны retry в канале) -> DLQ") {
        assertTrue(NotificationConsumer.isPoison(TelegramSendFailed("telegram down")))
      },
      test("PersistenceError (транзиентная) -> НЕ poison, поток переподписывается") {
        assertTrue(!NotificationConsumer.isPoison(PersistenceError("db connection lost")))
      },
      test("прочие доменные ошибки -> НЕ poison") {
        assertTrue(!NotificationConsumer.isPoison(TaskNotFound("1")))
        assertTrue(!NotificationConsumer.isPoison(BusinessRuleViolation("rule")))
        assertTrue(!NotificationConsumer.isPoison(UserNotFound("1")))
      }
    )
