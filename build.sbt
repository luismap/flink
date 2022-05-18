ThisBuild / scalaVersion := "2.12.15"
ThisBuild / organization := "com.example"

val akkaVersion = "2.5.13"

lazy val hello = (project in file("."))
  .settings(
    name := "flink",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
      "org.scalatest" %% "scalatest" % "3.0.8" % "test"
    )
  )

