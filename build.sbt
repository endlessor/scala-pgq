name := """scala-pgq"""

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.scalikejdbc" %% "scalikejdbc" % "2.1.2",
  "postgresql" % "postgresql" % "9.1-901.jdbc4",
  "com.typesafe.akka" %% "akka-actor" % "2.4.2",
  "com.typesafe.akka" %% "akka-stream" % "2.4.2",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.2",
  
  "org.scalatest" %% "scalatest" % "2.1.6" % "test",
  "ch.qos.logback" % "logback-classic" % "1.1.3" % "test"
)

fork := true