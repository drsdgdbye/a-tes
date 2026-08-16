package inc.uberpopug.taskservice.service

import inc.uberpopug.common.domain.UserId
import inc.uberpopug.taskservice.domain.Role

/** Верифицированная личность, приходящая в `X-Auth-User-Id` / `X-Auth-User-Role` от Gateway. */
final case class AuthenticatedUser(id: UserId, role: Role)
