package inc.uberpopug.taskservice.repository

import io.getquill.{SchemaMeta, schemaMeta}

/** Явный маппинг row-классов на таблицы БД: имена таблиц из Flyway-миграций (tasks, outbox, processed_events), а не
  * дефолтные `*_row` от Quill (SnakeCase). Импортируется в Live-реализациях репозиториев через `import Tables.given`.
  */
object Tables:
  inline implicit def taskRowSchema: SchemaMeta[TaskRow] = schemaMeta("tasks")
  inline implicit def outboxRowSchema: SchemaMeta[OutboxRow] = schemaMeta("outbox")
  inline implicit def processedEventsRowSchema: SchemaMeta[ProcessedEventRow] = schemaMeta("processed_events")
