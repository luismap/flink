
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / organization := "com.example"

val akkaVersion = "2.5.13"
val flinkVersion = "1.14.0"

lazy val hello = (project in file("."))
  .settings(
    name := "flink",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "org.scalatest" %% "scalatest" % "3.0.8" % "test",
      "org.apache.flink" %% "flink-scala" % flinkVersion,
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
      "org.apache.flink" %% "flink-clients" % flinkVersion


    )
  )

