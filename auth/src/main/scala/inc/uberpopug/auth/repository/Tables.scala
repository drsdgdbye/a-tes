package inc.uberpopug.auth.repository

import io.getquill.{SchemaMeta, schemaMeta}

/** Явный маппинг row-классов на таблицы БД: имена таблиц из Flyway-миграций (users, outbox, refresh_tokens), а не
  * дефолтные `*_row` от Quill (SnakeCase). Импортируется в Live-реализациях репозиториев через `import Tables.given`.
  */
object Tables:
  inline implicit def userRowSchema: SchemaMeta[UserRow] = schemaMeta("users")
  inline implicit def outboxRowSchema: SchemaMeta[OutboxRow] = schemaMeta("outbox")
  inline implicit def refreshTokenRowSchema: SchemaMeta[RefreshTokenRow] = schemaMeta("refresh_tokens")
