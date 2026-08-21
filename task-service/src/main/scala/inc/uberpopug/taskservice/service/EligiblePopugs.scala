package inc.uberpopug.taskservice.service

import zio.{Ref, UIO, ZIO, ZLayer}

import inc.uberpopug.common.domain.UserId
import inc.uberpopug.taskservice.repository.UserRepository

/** Кэш попугов, доступных для назначения. Наполняется из событий `UserCreated` (только роль `popug`) и персистируется в
  * таблицу `users` для выживания перезапусков.
  */
final case class EligiblePopugs(ref: Ref[Set[UserId]]):
  /** Добавляет пользователя в кэш (идемпотентно). */
  def add(userId: UserId): UIO[Unit] = ref.update(_ + userId)

  /** Текущее множество доступных попугов. */
  def all: UIO[Set[UserId]] = ref.get

object EligiblePopugs:
  /** Слой кэша, загружающий попугов из БД при старте. При ошибке БД стартует с пустым кэшем. */
  val layer: ZLayer[UserRepository, Nothing, EligiblePopugs] =
    ZLayer.fromZIO {
      for
        popugs <- ZIO
          .serviceWithZIO[UserRepository](_.findAllPopugs())
          .catchAll(_ => ZIO.succeed(List.empty[UserId]))
        ref <- Ref.make(popugs.toSet)
      yield EligiblePopugs(ref)
    }
