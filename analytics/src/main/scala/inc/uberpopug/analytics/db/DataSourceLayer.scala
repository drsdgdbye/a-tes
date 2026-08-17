package inc.uberpopug.analytics.db

import javax.sql.DataSource

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import zio.{ZIO, ZLayer}

import inc.uberpopug.analytics.config.{AppConfig, DatabaseConfig}

/** Слой пула соединений HikariCP поверх JDBC-URL из конфига. */
object DataSourceLayer:
  /** Создаёт пул при старте и закрывает его при финализации слоя. */
  val live: ZLayer[AppConfig, Throwable, DataSource] =
    ZLayer.scoped {
      for {
        cfg <- ZIO.service[AppConfig]
        _ <- ZIO.logInfo(s"Initializing HikariCP pool for ${cfg.database.url}")
        ds <- ZIO.attempt(makeHikari(cfg.database))
        _ <- ZIO.addFinalizer(ZIO.attempt(ds.close()).ignore)
      } yield ds
    }

  /** Конфигурирует и создаёт экземпляр HikariDataSource для Postgres. */
  private def makeHikari(db: DatabaseConfig): HikariDataSource =
    val hc = new HikariConfig()
    hc.setJdbcUrl(db.url)
    hc.setUsername(db.user)
    hc.setPassword(db.password)
    hc.setMaximumPoolSize(db.maxPoolSize)
    hc.setDriverClassName("org.postgresql.Driver")
    hc.setPoolName("ates-analytics-pool")
    new HikariDataSource(hc)
