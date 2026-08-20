package inc.uberpopug.analytics.repository

import io.getquill.{SchemaMeta, schemaMeta}

/** Явный маппинг row-классов на таблицы БД: имена таблиц из Flyway-миграций (tasks, popug_balances, daily_stats,
  * processed_events), а не дефолтные `*_row` от Quill (SnakeCase). Импортируется в Live-реализациях репозиториев через
  * `import Tables.given`.
  */
object Tables:
  inline implicit def taskRowSchema: SchemaMeta[TaskRow] = schemaMeta("tasks")
  inline implicit def popugBalanceRowSchema: SchemaMeta[PopugBalanceRow] = schemaMeta("popug_balances")
  inline implicit def dailyStatsRowSchema: SchemaMeta[DailyStatsRow] = schemaMeta("daily_stats")
  inline implicit def processedEventsRowSchema: SchemaMeta[ProcessedEventRow] = schemaMeta("processed_events")
