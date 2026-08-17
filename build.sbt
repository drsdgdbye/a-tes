ThisBuild / organization := "inc.uberpopug"
ThisBuild / version      := "0.2.0"
ThisBuild / scalaVersion := "3.8.4"

val zioVersion       = "2.1.26"
val zioJsonVersion   = "0.9.2"
val zioConfigVersion = "4.0.8"
val zioLoggingVersion = "2.5.3"
val zioKafkaVersion  = "3.7.1"
val tapirVersion     = "1.13.31"
val zioHttpVersion   = "3.11.3"
val zioMetricsVersion = "2.5.6"
val rezilienceVersion = "0.10.4"
val quillVersion     = "4.8.6"
val flywayVersion    = "13.3.0"
val hikariVersion    = "7.1.0"
val postgresVersion  = "42.7.13"
val nimbusVersion    = "10.9.1"
val bcryptVersion    = "0.10.2"
val logbackVersion   = "1.6.3"

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Wunused:all",
  "-Wvalue-discard",
  "-Werror"
)

ThisBuild / libraryDependencySchemes += "dev.zio" %% "zio-json" % VersionScheme.Always

lazy val common = project
  .in(file("common"))
  .settings(
    name := "a-tes-common",
    Compile / scalacOptions += "-Wconf:src=.*scalapb.*:silent",
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
    ),
    libraryDependencies ++= Seq(
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
    )
  )

lazy val auth = project
  .in(file("auth"))
  .settings(
    name := "a-tes-auth",
    libraryDependencies ++= Seq(
      "dev.zio"                 %% "zio"                            % zioVersion,
      "dev.zio"                 %% "zio-streams"                    % zioVersion,
      "dev.zio"                 %% "zio-json"                       % zioJsonVersion,
      "dev.zio"                 %% "zio-config"                     % zioConfigVersion,
      "dev.zio"                 %% "zio-config-magnolia"            % zioConfigVersion,
      "dev.zio"                 %% "zio-config-typesafe"            % zioConfigVersion,
      "dev.zio"                 %% "zio-logging"                    % zioLoggingVersion,
      "dev.zio"                 %% "zio-logging-slf4j2-bridge"      % zioLoggingVersion,
      "dev.zio"                 %% "zio-kafka"                      % zioKafkaVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-zio"                  % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-zio-http-server"      % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-json-zio"             % tapirVersion,
      "io.getquill"             %% "quill-jdbc-zio"                 % quillVersion,
      "org.flywaydb"             % "flyway-core"                    % flywayVersion,
      "org.flywaydb"             % "flyway-database-postgresql"     % flywayVersion,
      "com.zaxxer"               % "HikariCP"                       % hikariVersion,
      "org.postgresql"           % "postgresql"                     % postgresVersion,
      "com.nimbusds"             % "nimbus-jose-jwt"                % nimbusVersion,
      "at.favre.lib"             % "bcrypt"                         % bcryptVersion,
      "com.thesamet.scalapb"    %% "scalapb-runtime"                % scalapb.compiler.Version.scalapbVersion,
      "ch.qos.logback"           % "logback-classic"                % logbackVersion % Runtime,
      "dev.zio"                 %% "zio-test"                       % zioVersion % Test,
      "dev.zio"                 %% "zio-test-sbt"                   % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Compile / mainClass := Some("inc.uberpopug.auth.Main"),
    Docker / packageName := "ates-auth",
    dockerBaseImage := "eclipse-temurin:21-jre",
    dockerExposedPorts := Seq(10001),
    dockerUpdateLatest := true,
    Universal / javaOptions ++= Seq("-Duser.timezone=UTC")
  )
  .dependsOn(common)
  .enablePlugins(JavaAppPackaging, DockerPlugin)

lazy val taskService = project
  .in(file("task-service"))
  .settings(
    name := "a-tes-task-service",
    libraryDependencies ++= Seq(
      "dev.zio"                    %% "zio"                            % zioVersion,
      "dev.zio"                    %% "zio-streams"                    % zioVersion,
      "dev.zio"                    %% "zio-json"                       % zioJsonVersion,
      "dev.zio"                    %% "zio-config"                     % zioConfigVersion,
      "dev.zio"                    %% "zio-config-magnolia"            % zioConfigVersion,
      "dev.zio"                    %% "zio-config-typesafe"            % zioConfigVersion,
      "dev.zio"                    %% "zio-logging"                    % zioLoggingVersion,
      "dev.zio"                    %% "zio-logging-slf4j2-bridge"      % zioLoggingVersion,
      "dev.zio"                    %% "zio-kafka"                      % zioKafkaVersion,
      "dev.zio"                    %% "zio-metrics-connectors"         % zioMetricsVersion,
      "dev.zio"                    %% "zio-metrics-connectors-prometheus" % zioMetricsVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-zio"                     % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-zio-http-server"         % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-json-zio"                % tapirVersion,
      "io.getquill"                %% "quill-jdbc-zio"                 % quillVersion,
      "org.flywaydb"                % "flyway-core"                    % flywayVersion,
      "org.flywaydb"                % "flyway-database-postgresql"     % flywayVersion,
      "com.zaxxer"                  % "HikariCP"                       % hikariVersion,
      "org.postgresql"              % "postgresql"                     % postgresVersion,
      "com.thesamet.scalapb"       %% "scalapb-runtime"                % scalapb.compiler.Version.scalapbVersion,
      "ch.qos.logback"              % "logback-classic"                % logbackVersion % Runtime,
      "dev.zio"                    %% "zio-test"                       % zioVersion % Test,
      "dev.zio"                    %% "zio-test-sbt"                   % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Compile / mainClass := Some("inc.uberpopug.taskservice.Main"),
    Docker / packageName := "ates-task-service",
    dockerBaseImage := "eclipse-temurin:21-jre",
    dockerExposedPorts := Seq(10003),
    dockerUpdateLatest := true,
    Universal / javaOptions ++= Seq("-Duser.timezone=UTC")
  )
  .dependsOn(common)
  .enablePlugins(JavaAppPackaging, DockerPlugin)

lazy val accounting = project
  .in(file("accounting"))
  .settings(
    name := "a-tes-accounting",
    libraryDependencies ++= Seq(
      "dev.zio"                    %% "zio"                            % zioVersion,
      "dev.zio"                    %% "zio-streams"                    % zioVersion,
      "dev.zio"                    %% "zio-json"                       % zioJsonVersion,
      "dev.zio"                    %% "zio-config"                     % zioConfigVersion,
      "dev.zio"                    %% "zio-config-magnolia"            % zioConfigVersion,
      "dev.zio"                    %% "zio-config-typesafe"            % zioConfigVersion,
      "dev.zio"                    %% "zio-logging"                    % zioLoggingVersion,
      "dev.zio"                    %% "zio-logging-slf4j2-bridge"      % zioLoggingVersion,
      "dev.zio"                    %% "zio-kafka"                      % zioKafkaVersion,
      "dev.zio"                    %% "zio-metrics-connectors"         % zioMetricsVersion,
      "dev.zio"                    %% "zio-metrics-connectors-prometheus" % zioMetricsVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-zio"                     % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-zio-http-server"         % tapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-json-zio"                % tapirVersion,
      "io.getquill"                %% "quill-jdbc-zio"                 % quillVersion,
      "org.flywaydb"                % "flyway-core"                    % flywayVersion,
      "org.flywaydb"                % "flyway-database-postgresql"     % flywayVersion,
      "com.zaxxer"                  % "HikariCP"                       % hikariVersion,
      "org.postgresql"              % "postgresql"                     % postgresVersion,
      "com.thesamet.scalapb"       %% "scalapb-runtime"                % scalapb.compiler.Version.scalapbVersion,
      "io.github.jkobejs"          %% "zio-cron"                       % "1.0.1",
      "ch.qos.logback"              % "logback-classic"                % logbackVersion % Runtime,
      "dev.zio"                    %% "zio-test"                       % zioVersion % Test,
      "dev.zio"                    %% "zio-test-sbt"                   % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Compile / mainClass := Some("inc.uberpopug.accounting.Main"),
    Docker / packageName := "ates-accounting",
    dockerBaseImage := "eclipse-temurin:21-jre",
    dockerExposedPorts := Seq(10004),
    dockerUpdateLatest := true,
    Universal / javaOptions ++= Seq("-Duser.timezone=UTC")
  )
  .dependsOn(common)
  .enablePlugins(JavaAppPackaging, DockerPlugin)

lazy val analytics = project
  .in(file("analytics"))
  .settings(name := "a-tes-analytics")
  .dependsOn(common)

lazy val notification = project
  .in(file("notification"))
  .settings(name := "a-tes-notification")
  .dependsOn(common)

lazy val gateway = project
  .in(file("gateway"))
  .settings(
    name := "a-tes-gateway",
    libraryDependencies ++= Seq(
      "dev.zio"                %% "zio"                            % zioVersion,
      "dev.zio"                %% "zio-json"                       % zioJsonVersion,
      "dev.zio"                %% "zio-config"                     % zioConfigVersion,
      "dev.zio"                %% "zio-config-magnolia"            % zioConfigVersion,
      "dev.zio"                %% "zio-config-typesafe"            % zioConfigVersion,
      "dev.zio"                %% "zio-logging"                    % zioLoggingVersion,
      "dev.zio"                %% "zio-logging-slf4j2-bridge"      % zioLoggingVersion,
      "dev.zio"                %% "zio-http"                       % zioHttpVersion,
      "dev.zio"                %% "zio-metrics-connectors"         % zioMetricsVersion,
      "dev.zio"                %% "zio-metrics-connectors-prometheus" % zioMetricsVersion,
      "nl.vroste"              %% "rezilience"                     % rezilienceVersion,
      "com.nimbusds"           % "nimbus-jose-jwt"                 % nimbusVersion,
      "ch.qos.logback"         % "logback-classic"                 % logbackVersion % Runtime,
      "dev.zio"                %% "zio-test"                       % zioVersion % Test,
      "dev.zio"                %% "zio-test-sbt"                   % zioVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"),
    Compile / mainClass := Some("inc.uberpopug.gateway.Main"),
    Docker / packageName := "ates-gateway",
    dockerBaseImage := "eclipse-temurin:21-jre",
    dockerExposedPorts := Seq(10002),
    dockerUpdateLatest := true,
    Universal / javaOptions ++= Seq("-Duser.timezone=UTC")
  )
  .dependsOn(common)
  .enablePlugins(JavaAppPackaging, DockerPlugin)

lazy val root = project
  .in(file("."))
  .settings(
    name := "a-tes",
    Compile / publish / skip := true
  )
  .aggregate(common, auth, taskService, accounting, analytics, notification, gateway)