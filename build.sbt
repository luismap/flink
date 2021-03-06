
ThisBuild / scalaVersion := "2.12.5"
ThisBuild / organization := "com.example"

val akkaVersion = "2.5.13"
val flinkVersion = "1.14.0"

lazy val root = (project in file("."))
  .dependsOn(akka)
  .settings(
    name := "flink",
    assembly / mainClass := Some("Flink"),
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-scala" % flinkVersion,
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
      "org.apache.flink" %% "flink-clients" % flinkVersion,
      "org.apache.flink" %% "flink-connector-twitter" % flinkVersion,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "commons-logging" % "commons-logging" % "1.2",
      "joda-time" % "joda-time" % "2.10.6"
    )
  )

lazy val akka = (project in file("akka"))
  .settings(
    name := "akka",
    assembly / assemblyJarName := "my_akka",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "org.scalatest" %% "scalatest" % "3.0.8" % "test",
    )
  )