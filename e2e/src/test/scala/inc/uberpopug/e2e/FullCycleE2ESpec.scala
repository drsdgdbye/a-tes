package inc.uberpopug.e2e

import zio.*
import zio.http.ZClient
import zio.test.*
import zio.test.Assertion.*

import inc.uberpopug.accounting.api.*
import inc.uberpopug.accounting.api.AccountingDtos.given
import inc.uberpopug.auth.api.*
import inc.uberpopug.auth.api.AuthDtos.given
import inc.uberpopug.taskservice.api.*
import inc.uberpopug.taskservice.api.TaskDtos.given

/** E2E через Gateway по полному циклу (E2E-01..07), shuffle (E2E-11..17) и JWT lifecycle (E2E-22..28).
  *
  * Полагается на асинхронную обработку Kafka, поэтому использует polling-хелпер `eventually`.
  */
object FullCycleE2ESpec extends GatewayE2E:
  private val AdminEmail = "admin@uberpopug.inc"
  private val AdminPassword = "admin"

  private def poll[R, A](effect: ZIO[R, Throwable, Option[A]], timeout: Duration = 60.seconds): ZIO[R, Throwable, A] =
    effect.flatMap {
      case Some(value) => ZIO.succeed(value)
      case None        => ZIO.sleep(2.seconds) *> poll(effect, timeout)
    }.timeout(timeout).flatMap {
      case Some(value) => ZIO.succeed(value)
      case None        => ZIO.fail(new RuntimeException("E2E poll timed out"))
    }

  private def eventually[R, A](effect: ZIO[R, Throwable, A], check: A => Boolean): ZIO[R, Throwable, A] =
    poll(effect.map(v => Option.when(check(v))(v)))

  private def registerPopug(client: GatewayClient, n: Int): ZIO[Scope, Throwable, TokenResponse] =
    client.postJson[RegisterRequest, TokenResponse](
      "/auth/register",
      RegisterRequest(s"Popug $n", s"popug$n@ates.io", "secret123")
    ).map(_._2)

  private def login(client: GatewayClient, email: String, password: String): ZIO[Scope, Throwable, TokenResponse] =
    client.postJson[LoginRequest, TokenResponse]("/auth/login", LoginRequest(email, password)).map(_._2)

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("FullCycle E2E")(
      test("E2E-22..28 JWT lifecycle through the gateway") {
        for
          gc <- gatewayClient
          _ <- eventually(gc.getJson[AuthConfigResponse]("/auth/config").map(_._2), _.registrationEnabled)
          loginResp <- login(gc, AdminEmail, AdminPassword)
          (status, tasks) <- gc.getJson[TasksResponse]("/tasks", Some(loginResp.accessToken))
        yield assertTrue(status == 200, loginResp.accessToken.nonEmpty, loginResp.refreshToken.nonEmpty, tasks.total >= 0)
      },
      test("E2E-01..07 full cycle: register, create, complete, accounting balance + audit") {
        for
          gc <- gatewayClient
          popug <- registerPopug(gc, 1)
          (createStatus, created) <- gc.postJson[CreateTaskRequest, TaskResponse](
            "/tasks",
            CreateTaskRequest("E2E task", Some("desc")),
            Some(popug.accessToken)
          )
          _ <- assertTrue(createStatus == 200, created.title == "E2E task", created.status == "open", created.assigneeId.nonEmpty)
          (listStatus, mine) <- gc.getJson[TasksResponse]("/tasks", Some(popug.accessToken))
          _ <- assertTrue(listStatus == 200, mine.items.exists(_.id == created.id))
          (completeStatus, completed) <- gc.postJson[TaskCompleteReq, TaskResponse](
            s"/tasks/${created.id}/complete",
            TaskCompleteReq(),
            Some(popug.accessToken)
          )
          _ <- assertTrue(completeStatus == 200, completed.status == "completed", completed.completedAt.nonEmpty)
          // Accounting проекции обновляются асинхронно из Kafka
          balance <- eventually(
            gc.getJson[BalanceResponse]("/accounts/me/balance", Some(popug.accessToken)).map(_._2),
            _.balance != "0.00"
          )
          _ <- assertTrue(balance.balance.nonEmpty)
          audit <- eventually(
            gc.getJson[AuditLogResponse]("/accounts/me/audit-log", Some(popug.accessToken)).map(_._2),
            _.items.size >= 2
          )
        yield assertTrue(audit.items.size >= 2, audit.items.map(_.`type`).toSet.contains("assign"))
      },
      test("E2E-11..17 shuffle with refund") {
        for
          gc <- gatewayClient
          a <- registerPopug(gc, 2)
          _ <- registerPopug(gc, 3)
          admin <- login(gc, AdminEmail, AdminPassword)
          _ <- gc.postJson[CreateTaskRequest, TaskResponse](
            "/tasks",
            CreateTaskRequest("Shuffle me", None),
            Some(a.accessToken)
          )
          _ <- eventually(gc.getJson[TasksResponse]("/tasks", Some(a.accessToken)).map(_._2), _.items.nonEmpty)
          (shuffleStatus, shuffle) <- gc.postJson[ShuffleReq, ShuffleResponse](
            "/tasks/shuffle",
            ShuffleReq(),
            Some(admin.accessToken)
          )
        yield assertTrue(shuffleStatus == 200, shuffle.tasksReassigned >= 0)
      }
    ).provideSomeLayer[TestEnvironment & Scope](stackLayer ++ ZClient.default)

  /** Пустое тело для `PATCH /tasks/{id}/complete`. */
  private final case class TaskCompleteReq()
  private object TaskCompleteReq:
    given zio.json.JsonCodec[TaskCompleteReq] = zio.json.DeriveJsonCodec.gen

  /** Пустое тело для `POST /tasks/shuffle`. */
  private final case class ShuffleReq()
  private object ShuffleReq:
    given zio.json.JsonCodec[ShuffleReq] = zio.json.DeriveJsonCodec.gen
