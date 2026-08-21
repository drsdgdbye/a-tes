package inc.uberpopug.taskservice.repository

import java.time.Instant
import java.util.UUID

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.getquill.*
import zio.{ZIO, ZLayer}
import zio.test.*

import inc.uberpopug.taskservice.db.DbContext.Postgres
import inc.uberpopug.taskservice.db.{DbContext, Migrations}
import inc.uberpopug.taskservice.domain.TaskStatus

/** Интеграционные тесты DB-констрейнтов задач (M-TASK-06/07): CHECK `assign_fee_cents > 0` и
  * `complete_reward_cents > 0`. Требуют Docker: поднимают реальный Postgres 16 через testcontainers.
  */
object TaskRepositoryConstraintsSpec extends ZIOSpecDefault:

  /** Поднимает Postgres-контейнер, создаёт HikariCP-пул и применяет Flyway-миграции; поверх — Quill-контекст. */
  private val dsLayer: ZLayer[Any, Nothing, javax.sql.DataSource] =
    ZLayer.scoped {
      for
        container <- ZIO.acquireRelease(
          ZIO.attemptBlocking {
            val c = PostgreSQLContainer(databaseName = "ates_task", username = "ates", password = "ates")
            c.start()
            c
          }
        )(c => ZIO.attemptBlocking(c.stop()).ignore)
        _ <- ZIO.logInfo(s"Postgres test container started on ${container.jdbcUrl}")
        ds <- ZIO.attempt {
          val hc = new HikariConfig()
          hc.setJdbcUrl(container.jdbcUrl)
          hc.setUsername(container.username)
          hc.setPassword(container.password)
          hc.setMaximumPoolSize(5)
          hc.setDriverClassName("org.postgresql.Driver")
          new HikariDataSource(hc)
        }
        _ <- Migrations.migrate(ds)
      yield ds
    }.orDie

  private val testScope: ZLayer[Any, Nothing, Postgres] = dsLayer >>> DbContext.live

  private val rowTemplate = TaskRow(
    id = UUID.randomUUID(),
    title = "Constraint probe",
    description = None,
    status = TaskStatus.Open.wire,
    assigneeId = UUID.randomUUID(),
    assignFeeCents = 1000L,
    completeRewardCents = 2000L,
    createdAt = Instant.parse("2026-01-01T00:00:00Z"),
    completedAt = None,
    version = 1L
  )

  private def insertWith(ctx: Postgres, row: TaskRow): ZIO[Any, Throwable, Unit] =
    import ctx.*
    import Tables.given
    ctx.run(query[TaskRow].insertValue(lift(row))).unit

  /** `true`, если ошибка — нарушение целостности БД (SQLState `23xxx`, в т.ч. CHECK 23514). */
  private def isIntegrityViolation(error: Throwable): Boolean =
    val sqlState = error match
      case e: java.sql.SQLException => e.getSQLState
      case _                        => null
    sqlState != null && sqlState.startsWith("23")

  def spec: Spec[Any, Any] =
    suite("TaskRepository DB constraints")(
      test("accepts a valid task row (sanity)") {
        for
          ctx <- ZIO.service[Postgres]
          _ <- insertWith(ctx, rowTemplate)
        yield assertCompletes
      },
      test("M-TASK-06 rejects assign_fee_cents = 0 via CHECK constraint") {
        for
          ctx <- ZIO.service[Postgres]
          result <- insertWith(ctx, rowTemplate.copy(assignFeeCents = 0L)).either
        yield assertTrue(result.isLeft, result.left.toOption.exists(isIntegrityViolation))
      },
      test("M-TASK-07 rejects complete_reward_cents = 0 via CHECK constraint") {
        for
          ctx <- ZIO.service[Postgres]
          result <- insertWith(ctx, rowTemplate.copy(completeRewardCents = 0L)).either
        yield assertTrue(result.isLeft, result.left.toOption.exists(isIntegrityViolation))
      }
    ).provideLayerShared(testScope)
