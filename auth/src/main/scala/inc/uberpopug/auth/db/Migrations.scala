package inc.uberpopug.auth.db

import javax.sql.DataSource

import org.flywaydb.core.Flyway
import zio.ZIO

/** Применение Flyway-миграций из `db/migration` перед стартом сервиса. */
object Migrations:
  /** Выполняет все неприменённые миграции в блокирующем режиме. */
  def migrate(ds: DataSource): ZIO[Any, Throwable, Unit] =
    ZIO.attemptBlocking {
      Flyway.configure().dataSource(ds).load().migrate()
    }.unit
