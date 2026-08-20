package inc.uberpopug.accounting.repository

import io.getquill.{SchemaMeta, schemaMeta}

/** Явный маппинг row-классов на таблицы БД: имена таблиц из Flyway-миграций (events, account_balances, users,
  * processed_events, outbox), а не дефолтные `*_row` от Quill (SnakeCase). Импортируется в Live-реализациях
  * репозиториев через `import Tables.given`.
  */
object Tables:
  inline implicit def eventRowSchema: SchemaMeta[EventRow] = schemaMeta("events")
  inline implicit def balanceRowSchema: SchemaMeta[BalanceRow] = schemaMeta("account_balances")
  inline implicit def userRowSchema: SchemaMeta[UserRow] = schemaMeta("users")
  inline implicit def processedEventsRowSchema: SchemaMeta[ProcessedEventRow] = schemaMeta("processed_events")
  inline implicit def outboxRowSchema: SchemaMeta[OutboxRow] = schemaMeta("outbox")
