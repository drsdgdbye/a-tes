package inc.uberpopug.accounting.service

import inc.uberpopug.accounting.domain.Role
import inc.uberpopug.common.domain.UserId

/** Верифицированная личность, приходящая в `X-Auth-User-Id` / `X-Auth-User-Role` от Gateway. */
final case class AuthenticatedUser(id: UserId, role: Role)
