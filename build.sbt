ThisBuild / organization := "inc.uberpopug"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "3.8.4"

lazy val common = project
  .in(file("modules/common"))
  .settings(
    name := "a-tes-common",
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
    )
  )

lazy val auth = project
  .in(file("modules/auth"))
  .settings(name := "a-tes-auth")
  .dependsOn(common)

lazy val taskService = project
  .in(file("modules/task-service"))
  .settings(name := "a-tes-task-service")
  .dependsOn(common)

lazy val accounting = project
  .in(file("modules/accounting"))
  .settings(name := "a-tes-accounting")
  .dependsOn(common)

lazy val analytics = project
  .in(file("modules/analytics"))
  .settings(name := "a-tes-analytics")
  .dependsOn(common)

lazy val notification = project
  .in(file("modules/notification"))
  .settings(name := "a-tes-notification")
  .dependsOn(common)

lazy val gateway = project
  .in(file("modules/gateway"))
  .settings(name := "a-tes-gateway")
  .dependsOn(common)

lazy val root = project
  .in(file("."))
  .settings(
    name := "a-tes",
    Compile / publish / skip := true
  )
  .aggregate(common, auth, taskService, accounting, analytics, notification, gateway)