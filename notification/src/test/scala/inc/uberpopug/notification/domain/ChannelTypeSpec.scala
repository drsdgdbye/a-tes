package inc.uberpopug.notification.domain

import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.common.domain.DomainError.InvalidValue

/** Тесты парсинга каналов доставки: wire-значения, регистронезависимость и неизвестные каналы. */
object ChannelTypeSpec extends ZIOSpecDefault:

  def spec: Spec[Any, Any] =
    suite("ChannelType")(
      test("from('telegram') -> Telegram") {
        assert(ChannelType.from("telegram"))(isRight(equalTo(ChannelType.Telegram)))
      },
      test("wire-значение канала в нижнем регистре") {
        assertTrue(ChannelType.Telegram.wire == "telegram")
      },
      test("from регистронезависим и обрезает пробелы") {
        assert(ChannelType.from("  TELEGRAM "))(isRight(equalTo(ChannelType.Telegram)))
      },
      test("from('email') -> InvalidValue (канал ещё не реализован)") {
        assert(ChannelType.from("email"))(
          isLeft(isSubtype[InvalidValue](hasField("field", _.field, equalTo("channel"))))
        )
      },
      test("from('') -> InvalidValue") {
        assert(ChannelType.from(""))(isLeft(isSubtype[InvalidValue](anything)))
      }
    )
