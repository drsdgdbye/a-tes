package inc.uberpopug.e2e

import java.io.File

import com.dimafeng.testcontainers.DockerComposeContainer
import com.dimafeng.testcontainers.DockerComposeContainer.ComposeFile
import com.dimafeng.testcontainers.ExposedService
import org.testcontainers.containers.wait.strategy.Wait
import zio.*
import zio.http.{Client, ZClient}
import zio.test.*

/** Базовый E2E-контейнер: поднимает ядро стека из `docker-compose.yml` (все сервисы собраны локально через
  * `sbt Docker/publishLocal`), открывает порт Gateway и отдаёт base URL как `String` в среде. HTTP-клиент — отдельно
  * через `ZClient.default`.
  *
  * Перед запуском: `sbt Docker/publishLocal` для всех модулей. Требуется запущенный Docker daemon.
  */
trait GatewayE2E extends ZIOSpecDefault:

  protected def composePath: String = "../docker-compose.yml"

  protected def gatewayPort: Int = 10002

  /** Слой: поднимает compose-стек и отдаёт base URL Gateway как `String`. */
  protected val stackLayer: ZLayer[Any, Throwable, String] =
    ZLayer.scoped {
      for
        compose <- ZIO.acquireRelease(
          ZIO.attemptBlocking(
            DockerComposeContainer(
              ComposeFile(Left(new File(composePath))),
              exposedServices = Seq(
                ExposedService("gateway", gatewayPort, Wait.forHttp("/ready").forStatusCode(200).forPort(gatewayPort))
              )
            )
          )
        )(c => ZIO.attemptBlocking(c.stop()).ignore)
        host = compose.getServiceHost("gateway", gatewayPort)
        mappedPort = compose.getServicePort("gateway", gatewayPort)
        baseUrl = s"http://$host:$mappedPort"
        _ <- ZIO.logInfo(s"Gateway E2E stack up at $baseUrl")
      yield baseUrl
    }

  protected def gatewayClient: ZIO[Client & String, Nothing, GatewayClient] =
    for
      client <- ZIO.service[Client]
      base <- ZIO.service[String]
    yield GatewayClient(base, client)

  protected def provideStack: ZLayer[Scope, Throwable, Client & String] =
    ZLayer.make[Client & String](stackLayer, ZClient.default)
