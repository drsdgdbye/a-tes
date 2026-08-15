package inc.uberpopug.gateway.proxy

import zio.test.*

object RouteResolverSpec extends ZIOSpecDefault:
  def spec: Spec[Any, Any] =
    suite("RouteResolver")(
      test("resolves known paths to the right downstream") {
        assertTrue(
          RouteResolver.resolve("/auth/login") == Some(Downstream.Auth),
          RouteResolver.resolve("/auth/refresh") == Some(Downstream.Auth),
          RouteResolver.resolve("/users/me") == Some(Downstream.Auth),
          RouteResolver.resolve("/tasks/123") == Some(Downstream.TaskService),
          RouteResolver.resolve("/accounts/balance") == Some(Downstream.Accounting),
          RouteResolver.resolve("/analytics/stats") == Some(Downstream.Analytics)
        )
      },
      test("returns None for unknown paths") {
        assertTrue(RouteResolver.resolve("/nonexistent") == None, RouteResolver.resolve("") == None)
      },
      test("marks only auth public paths as public") {
        assertTrue(
          RouteResolver.isPublic("/auth/login"),
          RouteResolver.isPublic("/auth/register"),
          RouteResolver.isPublic("/auth/config"),
          RouteResolver.isPublic("/auth/refresh"),
          RouteResolver.isPublic("/auth/logout"),
          RouteResolver.isPublic("/auth/keys"),
          !RouteResolver.isPublic("/tasks/1"),
          !RouteResolver.isPublic("/users/me"),
          !RouteResolver.isPublic("/auth/profile")
        )
      }
    )
