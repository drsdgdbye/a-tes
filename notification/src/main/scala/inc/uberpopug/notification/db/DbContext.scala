package inc.uberpopug.notification.db

import javax.sql.DataSource

import io.getquill.SnakeCase
import io.getquill.jdbczio.Quill
import zio.ZLayer

/** Quill-контекст для Postgres: общий для всех репозиториев сервиса. */
object DbContext:
  /** Алиас контекста Postgres со snake_case-маппингом имён таблиц и колонок. */
  type Postgres = Quill.Postgres[SnakeCase]

  /** Слой Quill-контекста, построенный поверх DataSource. */
  val live: ZLayer[DataSource, Nothing, Postgres] = Quill.Postgres.fromNamingStrategy(SnakeCase)
