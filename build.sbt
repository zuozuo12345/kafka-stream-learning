val scala2Version = "2.13.8"
val globalVersion = "0.1.0-SNAPSHOT"

val deps = Seq(
  "org.scalactic" %% "scalactic" % "3.2.10",
  "org.scalatest" %% "scalatest" % "3.2.10" % "test",
  "org.scalacheck" %% "scalacheck" % "1.15.4" % "test",
  "org.apache.kafka" % "kafka-clients" % "3.1.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "3.1.0",
  "org.apache.kafka" %% "kafka" % "3.1.0",
  "org.slf4j" % "slf4j-simple" % "1.7.36",
  "org.postgresql" % "postgresql" % "42.3.2",
  "org.apache.kafka" % "kafka-streams-test-utils" % "3.1.0" % Test,

)

lazy val root = project
  .in(file("."))
  .settings(
    name := "project",
    version := globalVersion,
    scalaVersion := scala2Version,
    libraryDependencies ++= deps
  )
