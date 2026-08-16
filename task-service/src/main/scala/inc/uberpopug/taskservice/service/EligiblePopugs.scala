package inc.uberpopug.taskservice.service

import zio.{Ref, UIO, ZLayer}

import inc.uberpopug.common.domain.UserId

/** In-memory кэш попугов, доступных для назначения. Наполняется из событий `UserCreated` (только роль `popug`). */
final case class EligiblePopugs(ref: Ref[Set[UserId]]):
  /** Добавляет пользователя в кэш (идемпотентно). */
  def add(userId: UserId): UIO[Unit] = ref.update(_ + userId)

  /** Текущее множество доступных попугов. */
  def all: UIO[Set[UserId]] = ref.get

object EligiblePopugs:
  /** Слой кэша, стартующий с пустым множеством. */
  val layer: ZLayer[Any, Nothing, EligiblePopugs] =
    ZLayer.fromZIO(Ref.make(Set.empty[UserId]).map(EligiblePopugs(_)))
